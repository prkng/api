from prkng import create_app, notifications
from prkng.database import PostgresWrapper

import datetime
from flask import current_app
import json
import os
from redis import Redis
from rq_scheduler import Scheduler
from subprocess import check_call
import urllib2

scheduler = Scheduler('scheduled_jobs', connection=Redis(db=1))


def init_tasks(debug=True):
    now = datetime.datetime.now()
    stop_tasks()
    scheduler.schedule(scheduled_time=now, func=update_car2go, interval=120, result_ttl=240, repeat=None)
    if not debug:
        scheduler.schedule(scheduled_time=now, func=update_analytics, interval=120, result_ttl=240, repeat=None)
        scheduler.schedule(scheduled_time=now, func=update_free_spaces, interval=300, result_ttl=600, repeat=None)
        scheduler.schedule(scheduled_time=now, func=send_notifications, interval=300, result_ttl=600, repeat=None)
        scheduler.schedule(scheduled_time=datetime.datetime.combine(datetime.date.today() + datetime.timedelta(days=1), datetime.time(8)),
            func=run_backup, args=["prkng", "prkng"], interval=86400, result_ttl=172800, repeat=None)
        scheduler.schedule(scheduled_time=now, func=clear_expired_apple_device_ids, interval=86400,
            result_ttl=172800, repeat=None)

def stop_tasks():
    for x in scheduler.get_jobs():
        scheduler.cancel(x)

def run_backup(username, database):
    backup_dir = os.path.join(os.path.dirname(os.environ["PRKNG_SETTINGS"]), 'backup')
    file_name = 'prkng-{}.sql.gz'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)
    check_call('pg_dump -c -U {PG_USERNAME} {PG_DATABASE} | gzip > {}'.format(os.path.join(backup_dir, file_name),
        PG_USERNAME=username, PG_DATABASE=database),
        shell=True)
    return os.path.join(backup_dir, file_name)

def send_notifications():
    r = Redis(db=1)
    data = r.lrange('prkng:pushnotif', 0, -1)
    r.delete('prkng:pushnotif')

    for x in data:
        x = json.loads(x)
        if x["device_type"] == "ios":
            notifications.send_apple_notification(x["device_id"], x["text"])

def clear_expired_apple_device_ids():
    """
    Task to check for failed notification delivery attempts due to unregistered iOS device IDs.
    Purge these device IDs from our users.
    """
    queries = []
    for (device_id, fail_time) in notifications.get_apple_notification_failures():
        queries.append("""
            UPDATE users SET device_id = NULL
            WHERE device_id = '{device_id}'
                AND last_hello < '{dtime}'
        """.format(device_id=device_id, dtime=fail_time.isoformat()))
    if queries:
        CONFIG = create_app().config
        db = PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
        db.queries(queries)

def update_car2go():
    """
    Task to check with the car2go API, find moved cars and update their positions/slots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    queries = []

    insert_car2go = """
        INSERT INTO car2go (city, vin, name, long, lat, address, slot_id, in_lot)
            SELECT '{city}', '{vin}', '{name}', {long}, {lat}, '{address}', {slot_id}, {in_lot};
    """

    update_car2go = """
        UPDATE car2go SET since = NOW(), name = '{name}', long = {long}, lat = {lat}, address = '{address}',
            slot_id = {slot_id}, in_lot = {in_lot}, parked = true
        WHERE vin = '{vin}'
    """

    for city in ["montreal", "newyork"]:
        # grab data from car2go api
        c2city = city
        if c2city == "newyork":
            c2city = "newyorkcity"
        raw = urllib2.urlopen("https://www.car2go.com/api/v2.1/vehicles?loc={city}&format=json&oauth_consumer_key={key}".format(city=c2city, key=CONFIG["CAR2GO_CONSUMER"]))
        data = json.loads(raw.read())["placemarks"]

        raw = urllib2.urlopen("https://www.car2go.com/api/v2.1/parkingspots?loc={city}&format=json&oauth_consumer_key={key}".format(city=c2city, key=CONFIG["CAR2GO_CONSUMER"]))
        lot_data = json.loads(raw.read())["placemarks"]
        lots = [x["name"] for x in lot_data]

        # unpark stale entries in our database
        our_vins = db.query("SELECT vin FROM car2go WHERE city = '{city}'".format(city=city))
        our_vins = [x[0] for x in our_vins]
        parked_vins = db.query("SELECT vin FROM car2go WHERE city = '{city}' AND parked = true".format(city=city))
        parked_vins = [x[0] for x in parked_vins]
        their_vins = [x["vin"] for x in data]
        for x in parked_vins:
            if not x in their_vins:
                queries.append("UPDATE car2go SET since = NOW(), parked = false WHERE city = '{city}' AND vin = '{vin}'".format(city=city, vin=x))

        # create or update car2go tracking with new data
        for x in data:
            query = None

            # if the address matches a car2go reserved lot, don't bother with a slot
            if x["address"] in lots:
                slot_id = "NULL"
                in_lot = True
            # otherwise grab the most likely slot within 5m
            else:
                slot = db.query("""
                    SELECT id
                    FROM slots
                    WHERE city = '{city}'
                        AND ST_DWithin(
                            ST_Transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                            geom,
                            5
                        )
                    ORDER BY ST_Distance(st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857), geom)
                    LIMIT 1
                """.format(city=city, x=x["coordinates"][0], y=x["coordinates"][1]))
                slot_id = slot[0][0] if slot else "NULL"
                in_lot = False

            # update or insert
            if x["vin"] in our_vins and not x["vin"] in parked_vins:
                query = update_car2go.format(
                    vin=x["vin"], name=x["name"], long=x["coordinates"][0], lat=x["coordinates"][1],
                    address=x["address"].replace("'", "''").encode('utf-8'), slot_id=slot_id,
                    in_lot=in_lot
                )
            elif not x["vin"] in our_vins:
                query = insert_car2go.format(
                    city=city, vin=x["vin"], name=x["name"], long=x["coordinates"][0], lat=x["coordinates"][1],
                    address=x["address"].replace("'", "''").encode('utf-8'), slot_id=slot_id,
                    in_lot=in_lot
                )
            if query:
                queries.append(query)

    db.queries(queries)


def update_free_spaces():
    """
    Task to check recently departed car2go spaces and record
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    start = datetime.datetime.now()
    finish = start - datetime.timedelta(minutes=5)

    db.query("""
        INSERT INTO free_spaces (slot_ids)
          SELECT array_agg(s.id) FROM slots s
            JOIN car2go c ON c.slot_id = s.id
            WHERE c.in_lot = false
              AND c.parked = false
              AND c.since  > '{}'
              AND c.since  < '{}'
    """.format(finish.strftime('%Y-%m-%d %H:%M:%S'), start.strftime('%Y-%m-%d %H:%M:%S')))


def update_analytics():
    """
    Task to push analytics submissions from Redis to DB
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    r = Redis(db=1)

    queries = []
    data = r.lrange('prkng:analytics:pos', 0, -1)
    r.delete('prkng:analytics:pos')

    for x in data:
        x = json.loads(x)
        queries.append("""
            INSERT INTO analytics_pos (user_id, lat, long, radius, created, search_type) VALUES ({}, {}, {}, {}, '{}', '{}')
        """.format(x["user_id"], x["lat"], x["long"], x["radius"], x["created"], x["search_type"]))

    data = r.lrange('prkng:analytics:event', 0, -1)
    r.delete('prkng:analytics:event')

    for x in data:
        x = json.loads(x)
        queries.append("""
            INSERT INTO analytics_event (user_id, lat, long, created, event) VALUES ({}, {}, {}, '{}', '{}')
        """.format(x["user_id"], x["lat"] or "NULL", x["long"] or "NULL", x["created"], x["event"]))

    db.queries(queries)
