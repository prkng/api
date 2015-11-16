from prkng import create_app, notifications
from prkng.database import PostgresWrapper

import datetime
import demjson
from flask import current_app
import json
import os
from redis import Redis
import requests
from rq_scheduler import Scheduler
from subprocess import check_call

scheduler = Scheduler('scheduled_jobs', connection=Redis(db=1))


def init_tasks(debug=True):
    now = datetime.datetime.now()
    stop_tasks()
    scheduler.schedule(scheduled_time=now, func=update_car2go, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_automobile, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_communauto, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_analytics, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_parkingpanda, interval=120, result_ttl=240, repeat=None)
    if not debug:
        scheduler.schedule(scheduled_time=now, func=update_free_spaces, interval=300, result_ttl=600, repeat=None)
        scheduler.schedule(scheduled_time=now, func=send_notifications, interval=300, result_ttl=600, repeat=None)
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

    for city in ["montreal", "newyork"]:
        # grab data from car2go api
        c2city = city
        if c2city == "newyork":
            c2city = "newyorkcity"
        raw = requests.get("https://www.car2go.com/api/v2.1/vehicles",
            params={"loc": c2city, "format": "json", "oauth_consumer_key": CONFIG["CAR2GO_CONSUMER"]})
        data = raw.json()["placemarks"]

        raw = requests.get("https://www.car2go.com/api/v2.1/parkingspots",
            params={"loc": c2city, "format": "json", "oauth_consumer_key": CONFIG["CAR2GO_CONSUMER"]})
        lot_data = raw.json()["placemarks"]

        # create or update car2go parking lots
        values = ["('{}','{}',{},{})".format(city, x["name"].replace("'", "''").encode("utf-8"),
            x["totalCapacity"], (x["totalCapacity"] - x["usedCapacity"])) for x in lot_data]
        if values:
            db.query("""
                UPDATE carshare_lots l SET capacity = d.capacity, available = d.available
                FROM (VALUES {}) AS d(city, name, capacity, available)
                WHERE l.company = 'car2go' AND l.city = d.city AND l.name = d.name
                    AND l.available != d.available
            """.format(",".join(values)))

        values = ["('{}','{}',{},{},'SRID=4326;POINT({} {})'::geometry)".format(city,
            x["name"].replace("'", "''").encode("utf-8"), x["totalCapacity"],
            (x["totalCapacity"] - x["usedCapacity"]), x["coordinates"][0],
            x["coordinates"][1]) for x in lot_data]
        if values:
            db.query("""
                INSERT INTO carshare_lots (company, city, name, capacity, available, geom, geojson)
                    SELECT 'car2go', d.city, d.name, d.capacity, d.available,
                            ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
                    FROM (VALUES {}) AS d(city, name, capacity, available, geom)
                    WHERE (SELECT 1 FROM carshare_lots l WHERE l.city = d.city AND l.name = d.name LIMIT 1) IS NULL
            """.format(",".join(values)))

        # unpark stale entries in our database
        db.query("""
            UPDATE carshares c SET since = NOW(), parked = false
            WHERE c.company = 'car2go'
                AND c.city = '{city}'
                AND c.parked = true
                AND (SELECT 1 FROM (VALUES {data}) AS d(pid) WHERE c.vin = d.pid LIMIT 1) IS NULL
        """.format(city=city, data=",".join(["('{}')".format(x["vin"]) for x in data])))

        # create or update car2go tracking with new data
        values = ["('{}','{}','{}','{}',{},'SRID=4326;POINT({} {})'::geometry)".format(city, x["vin"],
            x["name"].encode('utf-8'), x["address"].replace("'", "''").encode("utf-8"),
            x["fuel"], x["coordinates"][0], x["coordinates"][1]) for x in data]
        db.query("""
            WITH tmp AS (
                SELECT DISTINCT ON (d.vin) d.vin, d.name, d.fuel, d.address, d.geom,
                    s.id AS slot_id, l.id AS lot_id
                FROM (VALUES {}) AS d(city, vin, name, address, fuel, geom)
                LEFT JOIN carshare_lots l ON d.city = l.city AND l.name = d.address
                LEFT JOIN slots s ON l.id IS NULL AND d.city = s.city
                    AND ST_DWithin(ST_Transform(d.geom, 3857), s.geom, 5)
                ORDER BY d.vin, ST_Distance(ST_Transform(d.geom, 3857), s.geom)
            )
            UPDATE carshares c SET since = NOW(), name = t.name, address = t.address,
                parked = true, slot_id = t.slot_id, lot_id = t.lot_id, fuel = t.fuel,
                geom = ST_Transform(t.geom, 3857), geojson = ST_AsGeoJSON(t.geom)::jsonb
            FROM tmp t
            WHERE c.company = 'car2go'
                AND c.vin = t.vin
                AND c.parked = false
        """.format(",".join(values)))
        db.query("""
            INSERT INTO carshares (company, city, vin, name, address, slot_id, lot_id, parked, fuel, geom, geojson)
                SELECT DISTINCT ON (d.vin) 'car2go', d.city, d.vin, d.name, d.address, s.id, l.id,
                    true, d.fuel, ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
                FROM (VALUES {}) AS d(city, vin, name, address, fuel, geom)
                LEFT JOIN carshare_lots l ON d.city = l.city AND l.name = d.address
                LEFT JOIN slots s ON l.id IS NULL AND s.city = d.city
                    AND ST_DWithin(ST_Transform(d.geom, 3857), s.geom, 5)
                WHERE (SELECT 1 FROM carshares c WHERE c.vin = d.vin LIMIT 1) IS NULL
                ORDER BY d.vin, ST_Distance(ST_Transform(d.geom, 3857), s.geom)
        """.format(",".join(values)))


def update_automobile():
    """
    Task to check with the Auto-mobile API, find moved cars and update their positions/slots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    # grab data from Auto-mobile api
    data = requests.get("https://www.reservauto.net/WCF/LSI/LSIBookingService.asmx/GetVehicleProposals",
        params={"Longitude": "-73.56307727766432", "Latitude": "45.48420949674474", "CustomerID": '""'})
    data = demjson.decode(data.text.lstrip("(").rstrip(");"))["Vehicules"]

    # unpark stale entries in our database
    db.query("""
        UPDATE carshares c SET since = NOW(), parked = false
        WHERE c.company = 'auto-mobile'
            AND c.parked = true
            AND (SELECT 1 FROM (VALUES {data}) AS d(pid) WHERE c.vin = d.pid LIMIT 1) IS NULL
    """.format(data=",".join(["('{}')".format(x["Id"]) for x in data])))

    # create or update Auto-mobile tracking with newly parked vehicles
    values = ["('{}','{}',{},'{}','SRID=4326;POINT({} {})'::geometry)".format(x["Id"],
        x["Immat"].encode('utf-8'), x["EnergyLevel"], int(x["Name"]), x["Position"]["Lon"],
        x["Position"]["Lat"]) for x in data]
    db.query("""
        WITH tmp AS (
            SELECT DISTINCT ON (d.vin) d.vin, d.name, d.fuel, d.id, s.id AS slot_id, s.way_name, d.geom
            FROM (VALUES {}) AS d(vin, name, fuel, id, geom)
            JOIN cities c ON ST_Intersects(ST_Transform(d.geom, 3857), c.geom)
            LEFT JOIN slots s ON s.city = c.name
                AND ST_DWithin(ST_Transform(d.geom, 3857), s.geom, 5)
            ORDER BY d.vin, ST_Distance(ST_Transform(d.geom, 3857), s.geom)
        )
        UPDATE carshares c SET partner_id = t.id, since = NOW(), name = t.name, address = t.way_name,
            parked = true, slot_id = t.slot_id, fuel = t.fuel, geom = ST_Transform(t.geom, 3857),
            geojson = ST_AsGeoJSON(t.geom)::jsonb
        FROM tmp t
        WHERE c.company = 'auto-mobile'
            AND c.vin = t.vin
            AND c.parked = false
    """.format(",".join(values)))

    values = ["('{}','{}',{},{},'{}','SRID=4326;POINT({} {})'::geometry)".format(x["Id"],
        x["Immat"].encode('utf-8'), x["EnergyLevel"], ("true" if x["Name"].endswith("-R") else "false"),
        int(x["Name"]), x["Position"]["Lon"], x["Position"]["Lat"]) for x in data]
    db.query("""
        INSERT INTO carshares (company, city, partner_id, vin, name, address, slot_id, parked, fuel, electric, geom, geojson)
            SELECT DISTINCT ON (d.vin) 'auto-mobile', c.name, d.id, d.vin, d.name, s.way_name, s.id,
                true, d.fuel, d.electric, ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
            FROM (VALUES {}) AS d(vin, name, fuel, electric, id, geom)
            JOIN cities c ON ST_Intersects(ST_Transform(d.geom, 3857), c.geom)
            LEFT JOIN slots s ON s.city = c.name
                AND ST_DWithin(ST_Transform(d.geom, 3857), s.geom, 5)
            WHERE (SELECT 1 FROM carshares c WHERE c.vin = d.vin LIMIT 1) IS NULL
            ORDER BY d.vin, ST_Distance(ST_Transform(d.geom, 3857), s.geom)
    """.format(",".join(values)))


def update_communauto():
    """
    Task to check with the Communuauto API, find moved cars and update their positions/slots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    for city in ["montreal", "quebec"]:
        # grab data from communauto api
        if city == "montreal":
            cacity = 59
        elif city == "quebec":
            cacity = 90
        start = datetime.datetime.now()
        finish = (start + datetime.timedelta(minutes=30))
        data = requests.post("https://www.reservauto.net/Scripts/Client/Ajax/PublicCall/Get_Car_DisponibilityJSON.asp",
            data={"CityID": cacity, "StartDate": start.strftime("%d/%m/%Y %H:%M"),
                "EndDate": finish.strftime("%d/%m/%Y %H:%M"), "FeeType": 80})
        # must use demjson here because returning format is non-standard JSON
        data = demjson.decode(data.text.lstrip("(").rstrip(")"))["data"]

        # create or update communauto parking spaces
        values = ["('{}',{})".format(x["StationID"], (1 if x["NbrRes"] == 0 else 0)) for x in data]
        db.query("""
            UPDATE carshare_lots l SET capacity = 1, available = d.available
            FROM (VALUES {}) AS d(pid, available)
            WHERE l.company = 'communauto'
                AND l.partner_id = d.pid
                AND l.available != d.available
        """.format(",".join(values)))

        values = ["('{}','{}',{},'{}','SRID=4326;POINT({} {})'::geometry)".format(city,
            x["strNomStation"].replace("'", "''").encode("utf-8"), (1 if x["NbrRes"] == 0 else 0),
            x["StationID"], x["Longitude"], x["Latitude"]) for x in data]
        db.query("""
            INSERT INTO carshare_lots (company, city, name, capacity, available, partner_id, geom, geojson)
                SELECT 'communauto', d.city, d.name, 1, d.available, d.partner_id,
                        ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
                FROM (VALUES {}) AS d(city, name, available, partner_id, geom)
                WHERE (SELECT 1 FROM carshare_lots l WHERE l.partner_id = d.partner_id LIMIT 1) IS NULL
        """.format(",".join(values)))

        # unpark stale entries in our database
        db.query("""
            UPDATE carshares c SET since = NOW(), parked = false
            FROM (VALUES {data}) AS d(pid, lot_id, numres)
            WHERE c.parked = true
                AND c.city = '{city}'
                AND d.numres = 1
                AND c.company = 'communauto'
                AND c.partner_id = d.pid;

            UPDATE carshares c SET since = NOW(), parked = false
            WHERE c.parked = true
                AND c.company = 'communauto'
                AND c.city = '{city}'
                AND (SELECT 1 FROM (VALUES {data}) AS d(pid, lot_id, numres) WHERE d.pid != c.partner_id
                     AND d.lot_id = c.lot_id LIMIT 1) IS NOT NULL
        """.format(city=city, data=",".join(["('{}',{},{})".format(x["CarID"],x["StationID"],x["NbrRes"]) for x in data])))

        # create or update communauto tracking with newly parked vehicles
        values = ["('{}',{},'{}','{}','{}'::timestamp,'SRID=4326;POINT({} {})'::geometry)".format(x["CarID"],
            x["NbrRes"], x["Model"].encode("utf-8"), x["strNomStation"].replace("'", "''").encode("utf-8"),
            x["AvailableUntilDate"] or "NOW", x["Longitude"], x["Latitude"]) for x in data]
        db.query("""
            UPDATE carshares c SET since = NOW(), until = d.until, name = d.name, address = d.address,
                parked = true, geom = ST_Transform(d.geom, 3857), geojson = ST_AsGeoJSON(d.geom)::jsonb
            FROM (VALUES {}) AS d(pid, numres, name, address, until, geom)
            WHERE c.company = 'communauto'
                AND c.partner_id = d.pid
                AND d.numres = 0
        """.format(",".join(values)))

        values = ["('{}',{},'{}','{}','{}',{},'{}'::timestamp,'SRID=4326;POINT({} {})'::geometry)".format(city,
            x["StationID"], x["CarID"], x["Model"].encode("utf-8"), x["strNomStation"].replace("'", "''").encode("utf-8"),
            x["NbrRes"], x["AvailableUntilDate"] or "NOW", x["Longitude"], x["Latitude"]) for x in data]
        db.query("""
            INSERT INTO carshares (company, city, partner_id, name, address, lot_id, parked, until, geom, geojson)
                SELECT 'communauto', d.city, d.partner_id, d.name, d.address, l.id, d.numres = 0,
                        d.until, ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
                FROM (VALUES {}) AS d(city, lot_pid, partner_id, name, address, numres, until, geom)
                JOIN carshare_lots l ON l.company = 'communauto' AND l.city = d.city
                    AND l.partner_id = d.lot_pid
                WHERE (SELECT 1 FROM carshares c WHERE c.partner_id = d.partner_id LIMIT 1) IS NULL
        """.format(",".join(values)))


def update_parkingpanda():
    """
    Task to check with the Parking Panda API, update data on associated parking lots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    for city, addr in [("newyork", "4 Pennsylvania Plaza, New York, NY")]:
        # grab data from parkingpanda api
        start = datetime.datetime.now()
        finish = (start + datetime.timedelta(hours=23, minutes=59))
        data = requests.get("https://www.parkingpanda.com/api/v2/locations",
            params={"search": addr, "miles": 20.0, "startTime": start.strftime("%H:%M"),
                "endDate": finish.strftime("%m/%d/%Y"), "endTime": finish.strftime("%H:%M"),
                "onlyavailable": False, "showSoldOut": True, "peer": False})
        data = data.json()["data"]["locations"]

        hourToFloat = lambda x: float(x.split(":")[0]) + (float(x.split(":")[1][0:2]) / 60) + (12 if "PM" in x and x.split(":")[0] != "12" else 0)
        values = []
        for x in data:
            x["displayName"] = x["displayName"].replace("'","''").encode("utf-8")
            x["displayAddress"] = x["displayAddress"].replace("'","''").encode("utf-8")
            x["description"] = x["description"].replace("'","''").encode("utf-8")
            basic = "({},'{}','{}',{},{},'{}','{}','SRID=4326;POINT({} {})'::geometry,'{}'::jsonb,'{}'::jsonb)"
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
                    d.agenda, d.attrs, p.street_view
                FROM (VALUES {}) AS d(pid, city, name, active, available, address, description,
                    geom, agenda, attrs)
                LEFT JOIN parking_lots_streetview p ON p.partner_name = 'Parking Panda' AND p.partner_id = d.pid
                WHERE (SELECT 1 FROM parking_lots l WHERE l.partner_id = d.pid LIMIT 1) IS NULL
            """.format(",".join(values)))


def update_free_spaces():
    """
    Task to check recently departed carshare spaces and record
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
            JOIN carshares c ON c.slot_id = s.id
            WHERE c.lot_id IS NULL
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
                    ST_Collect(ST_Transform(ST_SetSRID(ST_MakePoint(lat, long), 4326), 3857)) AS geom
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
