# -*- coding: utf-8 -*-

from prkng import create_app, notifications
from prkng.database import PostgresWrapper

import boto.sns
from boto.exception import BotoServerError
from boto.s3.connection import S3Connection
import datetime
import json
import os
import pytz
import re
from redis import Redis
import requests
from rq import Queue
import subprocess


def process_notifications():
    q = Queue('medium', connection=Redis(db=1))
    q.enqueue(hello_amazon)
    q.enqueue(send_notifications)

def update_lots():
    q = Queue('medium', connection=Redis(db=1))
    q.enqueue(update_parkingpanda)
    q.enqueue(update_seattle_lots)


def hello_amazon():
    """
    Fetch newly-registered users' device IDs and register with Amazon SNS for push notifications.
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    r = Redis(db=1)
    amz = boto.sns.connect_to_region("us-west-2",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY"],
        aws_secret_access_key=CONFIG["AWS_SECRET_KEY"])
    values = []

    # register the user's device ID with Amazon, and add to the associated notification topics
    for d in ["ios", "ios-sbx", "android"]:
        for x in r.hkeys('prkng:hello-amazon:'+d):
            try:
                device_id = r.hget('prkng:hello-amazon:'+d, x)
                arn = amz.create_platform_endpoint(CONFIG["AWS_SNS_APPS"][d], device_id, x.encode('utf-8'))
                arn = arn['CreatePlatformEndpointResponse']['CreatePlatformEndpointResult']['EndpointArn']
                values.append("({},'{}')".format(x, arn))
                r.hdel('prkng:hello-amazon:'+d, x)
                if not CONFIG["DEBUG"]:
                    amz.subscribe(CONFIG["AWS_SNS_TOPICS"]["all_users"], "application", arn)
                    amz.subscribe(CONFIG["AWS_SNS_TOPICS"][d+"_users"], "application", arn)
            except Exception, e:
                if "already exists with the same Token" in e.message:
                    arn = re.search("Endpoint (arn:aws:sns\S*)\s.?", e.message)
                    if not arn:
                        continue
                    values.append("({},'{}')".format(x, arn.group(1)))
                    r.hdel('prkng:hello-amazon:'+d, x)

    # Update the local user records with their new Amazon SNS ARNs
    if values:
        db.query("""
            UPDATE users u SET sns_id = d.arn
            FROM (VALUES {}) AS d(uid, arn)
            WHERE u.id = d.uid
        """.format(",".join(values)))


def send_notifications():
    """
    Send a push notification to specified user IDs via Amazon SNS
    """
    CONFIG = create_app().config
    r = Redis(db=1)
    amz = boto.sns.connect_to_region("us-west-2",
        aws_access_key_id=CONFIG["AWS_ACCESS_KEY"],
        aws_secret_access_key=CONFIG["AWS_SECRET_KEY"])

    keys = r.hkeys('prkng:push')
    if not keys:
        return

    for pid in keys:
        message = r.hget('prkng:push', pid)
        r.hdel('prkng:push', pid)
        device_ids = r.lrange('prkng:push:'+pid, 0, -1)
        r.delete('prkng:push:'+pid)

        message_structure = None
        if message.startswith("{") and message.endswith("}"):
            message_structure = "json"
        mg_title = "message-group-{}".format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
        mg_arn = None

        if device_ids == ["all"]:
            # Automatically publish messages destined for "all" via our All Users notification topic
            amz.publish(message=message, message_structure=message_structure,
                target_arn=CONFIG["AWS_SNS_TOPICS"]["all_users"])
        elif device_ids == ["ios"]:
            # Automatically publish messages destined for all iOS users
            amz.publish(message=message, message_structure=message_structure,
                target_arn=CONFIG["AWS_SNS_TOPICS"]["ios_users"])
        elif device_ids == ["android"]:
            # Automatically publish messages destined for all Android users
            amz.publish(message=message, message_structure=message_structure,
                target_arn=CONFIG["AWS_SNS_TOPICS"]["android_users"])
        elif device_ids == ["en"]:
            # Automatically publish messages destined for all English-language users
            amz.publish(message=message, message_structure=message_structure,
                target_arn=CONFIG["AWS_SNS_TOPICS"]["en_users"])
        elif device_ids == ["fr"]:
            # Automatically publish messages destined for all French-language users
            amz.publish(message=message, message_structure=message_structure,
                target_arn=CONFIG["AWS_SNS_TOPICS"]["fr_users"])

        if len(device_ids) >= 10:
            # If more than 10 real device IDs at once:
            for id in device_ids:
                if id.startswith("arn:aws:sns") and "endpoint" in id:
                    # this is a user device ID
                    # Create a temporary topic for a manually specified list of users
                    if not mg_arn:
                        mg_arn = amz.create_topic(mg_title)
                        mg_arn = mg_arn["CreateTopicResponse"]["CreateTopicResult"]["TopicArn"]
                    try:
                        amz.subscribe(mg_arn, "application", id)
                    except:
                        continue
                elif id.startswith("arn:aws:sns"):
                    # this must be a topic ARN, send to it immediately
                    amz.publish(message=message, message_structure=message_structure, target_arn=id)
            if mg_arn:
                # send to all user device IDs that we queued up in the prior loop
                amz.publish(message=message, message_structure=message_structure, target_arn=mg_arn)
        else:
            # Less than 10 device IDs or topic ARNs. Send to them immediately
            for id in [x for x in device_ids if x.startswith("arn:aws:sns")]:
                try:
                    amz.publish(message=message, message_structure=message_structure, target_arn=id)
                except BotoServerError:
                    continue


def update_parkingpanda():
    """
    Task to check with the Parking Panda API, update data on associated parking lots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    parkingpanda_url = "https://www.parkingpanda.com/api/v2/locations" if not CONFIG["DEBUG"] else "http://dev.parkingpanda.com/api/v2/locations"

    for city, addr in [("newyork", "4 Pennsylvania Plaza, New York, NY")]:
        # grab data from parkingpanda api
        start = datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
        finish = (start + datetime.timedelta(hours=23, minutes=59))
        data = requests.get(parkingpanda_url, params={"search": addr, "miles": 20.0,
                "startDate": start.strftime("%m/%d/%Y"), "startTime": start.strftime("%H:%M"),
                "endDate": finish.strftime("%m/%d/%Y"), "endTime": finish.strftime("%H:%M"),
                "onlyavailable": False, "showSoldOut": True, "peer": False})
        data = data.json()["data"]["locations"]

        hourToFloat = lambda x: float(x.split(":")[0]) + (float(x.split(":")[1][0:2]) / 60) + (12 if "PM" in x and x.split(":")[0] != "12" else 0)
        values = []
        for x in data:
            x["displayName"] = x["displayName"].replace("'","''").encode("utf-8")
            x["displayAddress"] = x["displayAddress"].replace("'","''").encode("utf-8")
            x["description"] = x["description"].replace("'","''").encode("utf-8")
            basic = "('{}','{}','{}',{},{},'{}','{}','SRID=4326;POINT({} {})'::geometry,'{}'::jsonb,'{}'::jsonb)"
            if x["isOpen247"]:
                agenda = {str(y): [{"max": None, "hourly": None, "daily": x["price"],
                    "hours": [0.0,24.0]}] for y in range(1,8)}
            else:
                agenda = {str(y): [] for y in range(1,8)}
                for y in x["hoursOfOperation"]:
                    if not y["isOpen"]:
                        continue
                    hours = [hourToFloat(y["timeOfDayOpen"]), hourToFloat(y["timeOfDayClose"])]
                    if y["timeOfDayClose"] == "11:59 PM":
                        hours[1] = 24.0
                    if hours != [0.0, 24.0] and hours[0] > hours[1]:
                        nextday = str(y["dayOfWeek"]+2) if (y["dayOfWeek"] < 6) else "1"
                        agenda[nextday].append({"max": None, "hourly": None,
                            "daily": x["price"], "hours": [0.0, hours[1]]})
                        hours = [hours[0], 24.0]
                    agenda[str(y["dayOfWeek"]+1)].append({"max": None, "hourly": None,
                        "daily": x["price"], "hours": hours})
            # Create "closed" rules for periods not covered by an open rule
            for j in agenda:
                hours = sorted([y["hours"] for y in agenda[j]], key=lambda z: z[0])
                for i, y in enumerate(hours):
                    starts = [z[0] for z in hours]
                    if y[0] == 0.0:
                        continue
                    last_end = hours[i-1][1] if not i == 0 else 0.0
                    next_start = hours[i+1][0] if not i == (len(hours) - 1) else 24.0
                    if not last_end in starts:
                        agenda[j].append({"hours": [last_end, y[0]], "hourly": None, "max": None,
                            "daily": None})
                    if not next_start in starts and y[1] != 24.0:
                        agenda[j].append({"hours": [y[1], next_start], "hourly": None, "max": None,
                            "daily": None})
                if agenda[j] == []:
                    agenda[j].append({"hours": [0.0,24.0], "hourly": None, "max": None, "daily": None})
            attrs = {"card": True, "indoor": "covered" in [y["name"] for y in x["amenities"]],
                "handicap": "accessible" in [y["name"] for y in x["amenities"]],
                "valet": "valet" in [y["name"] for y in x["amenities"]]}
            values.append(basic.format(x["id"], city, x["displayName"], json.dumps(x["isLive"]),
                x["availableSpaces"], x["displayAddress"], x["description"],
                x["longitude"], x["latitude"], json.dumps(agenda), json.dumps(attrs)))

        if values:
            db.query("""
                UPDATE parking_lots l SET available = d.available, agenda = d.agenda, attrs = d.attrs,
                    active = d.active
                FROM (VALUES {}) AS d(pid, city, name, active, available, address, description,
                    geom, agenda, attrs)
                WHERE l.partner_name = 'Parking Panda'
                    AND l.partner_id = d.pid
            """.format(",".join(values)))
            db.query("""
                INSERT INTO parking_lots (partner_id, partner_name, city, name, active,
                    available, address, description, geom, geojson, agenda, attrs, street_view)
                SELECT d.pid, 'Parking Panda', d.city, d.name, d.active, d.available, d.address,
                    d.description, ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb,
                    d.agenda, d.attrs, json_build_object('head', p.street_view_head, 'id', p.street_view_id)::jsonb
                FROM (VALUES {}) AS d(pid, city, name, active, available, address, description,
                    geom, agenda, attrs)
                LEFT JOIN parking_lots_streetview p ON p.partner_name = 'Parking Panda' AND p.partner_id = d.pid
                WHERE (SELECT 1 FROM parking_lots l WHERE l.partner_id = d.pid LIMIT 1) IS NULL
            """.format(",".join(values)))


def update_seattle_lots():
    """
    Fetch Seattle parking lot data and real-time availability from City of Seattle GIS
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    # grab data from city of seattle dot
    data = requests.get("http://web6.seattle.gov/sdot/wsvcEparkGarageOccupancy/Occupancy.asmx/GetGarageList",
        params={"prmGarageID": "G", "prmMyCallbackFunctionName": ""})
    data = json.loads(data.text.lstrip("(").rstrip(");"))

    if data:
        db.query("""
            UPDATE parking_lots l SET available = d.available
            FROM (VALUES {}) AS d(pid, available)
            WHERE l.partner_name = 'Seattle ePark'
                AND l.partner_id = d.pid
        """.format(",".join(["('{}',{})".format(x["Id"], x["VacantSpaces"]) for x in data])))


def run_backup():
    CONFIG = create_app().config
    file_name = 'prkng-{}.sql.gz'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))

    c = S3Connection(CONFIG["AWS_ACCESS_KEY"], CONFIG["AWS_SECRET_KEY"])

    subprocess.check_call('pg_dump -c -U {PG_USERNAME} {PG_DATABASE} | gzip > {file_name}'.format(
        file_name=os.path.join('/tmp', file_name), **CONFIG),
        shell=True)

    b = c.get_bucket('prkng-bak')
    k = b.initiate_multipart_upload(file_name, encrypt_key=True)
    with open(os.path.join('/tmp', file_name), 'rb') as f:
        k.upload_part_from_file(f, 1)
    k.complete_upload()
    os.unlink(os.path.join('/tmp', file_name))
    return os.path.join('prkng-bak/', file_name)


def update_analytics():
    """
    Task to push analytics submissions from Redis to DB
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    r = Redis(db=1)

    data = r.lrange('prkng:analytics:pos', 0, -1)
    r.delete('prkng:analytics:pos')

    values = ["({}, {}, {}, '{}'::timestamp, '{}')".format(x["user_id"], x["lat"], x["long"],
        x["created"], x["search_type"]) for x in map(lambda y: json.loads(y), data)]
    if values:
        pos_query = """
            WITH tmp AS (
                SELECT
                    user_id,
                    search_type,
                    count(*),
                    date_trunc('hour', created) AS hour_stump,
                    (extract(minute FROM created)::int / 5) AS min_by5,
                    ST_Collect(ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3857)) AS geom
                FROM (VALUES {}) AS d(user_id, lat, long, created, search_type)
                GROUP BY 1, 2, 4, 5
                ORDER BY 1, 2, 4, 5
            )
            INSERT INTO analytics_pos (user_id, geom, centerpoint, count, created, search_type)
                SELECT user_id, geom, ST_Centroid(geom), count, hour_stump + (INTERVAL '5 MINUTES' * min_by5),
                    search_type
                FROM tmp
        """.format(",".join(values))
        db.query(pos_query)

    data = r.lrange('prkng:analytics:event', 0, -1)
    r.delete('prkng:analytics:event')

    if data:
        event_query = "INSERT INTO analytics_event (user_id, lat, long, created, event) VALUES "
        event_query += ",".join(["({}, {}, {}, '{}', '{}')".format(x["user_id"], x["lat"] or "NULL",
            x["long"] or "NULL", x["created"], x["event"]) for x in map(lambda y: json.loads(y), data)])
        db.query(event_query)
