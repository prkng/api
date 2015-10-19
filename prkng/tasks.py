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
import urllib2

scheduler = Scheduler('scheduled_jobs', connection=Redis(db=1))


def init_tasks(debug=True):
    now = datetime.datetime.now()
    stop_tasks()
    scheduler.schedule(scheduled_time=now, func=update_car2go, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_automobile, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_communauto, interval=120, result_ttl=240, repeat=None)
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
        INSERT INTO carshares (company, city, vin, name, address, slot_id, lot_id, parked, fuel, geom, geojson)
            SELECT 'car2go', '{city}', '{vin}', '{name}', '{address}', {slot_id}, {lot_id}, true, {fuel},
                    ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
                    ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb;
    """

    update_car2go = """
        UPDATE carshares SET since = NOW(), name = '{name}', address = '{address}', slot_id = {slot_id},
            lot_id = {lot_id}, parked = true, fuel = {fuel},
            geom = ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
            geojson = ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb
        WHERE vin = '{vin}'
    """

    insert_lot = """
        INSERT INTO carshare_lots (company, city, name, capacity, available, geom, geojson)
            SELECT 'car2go', '{city}', '{name}', {capacity}, {available},
                    ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
                    ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb;
    """

    update_lot = """
        UPDATE carshare_lots SET capacity = {capacity}, available = {available}
        WHERE city = '{city}' AND name = '{name}'
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
        lots = [x["name"].replace("'", "''").encode("utf-8") for x in lot_data]

        our_lots = db.query("SELECT name FROM carshare_lots WHERE city = '{city}'".format(city=city))
        our_lots = [x[0] for x in our_lots] if our_lots else []
        for x in lot_data:
            x["name"] = x["name"].replace("'", "''").encode("utf-8")
            if x["name"] in our_lots:
                queries.append(update_lot.format(city=city, name=x["name"], capacity=x["totalCapacity"],
                    available=x["totalCapacity"] - x["usedCapacity"]))
            else:
                queries.append(insert_lot.format(city=city, name=x["name"], capacity=x["totalCapacity"],
                    available=x["totalCapacity"] - x["usedCapacity"], long=x["coordinates"][0], lat=x["coordinates"][1]))
        db.queries(queries)
        queries = []

        # unpark stale entries in our database
        our_vins = db.query("SELECT vin FROM carshares WHERE company = 'car2go' AND city = '{city}'".format(city=city))
        our_vins = [x[0] for x in our_vins] if our_vins else []
        parked_vins = db.query("SELECT vin FROM carshares WHERE company = 'car2go' AND city = '{city}' AND parked = true".format(city=city))
        parked_vins = [x[0] for x in parked_vins] if parked_vins else []
        their_vins = [x["vin"] for x in data]
        for x in parked_vins:
            if not x in their_vins:
                queries.append("UPDATE carshares SET since = NOW(), parked = false WHERE city = '{city}' AND vin = '{vin}'".format(city=city, vin=x))

        # create or update car2go tracking with new data
        for x in data:
            query = None
            x["address"] = x["address"].replace("'", "''").encode("utf-8")

            # if the address matches a car2go reserved lot, don't bother with a slot
            if x["address"] in lots:
                lot = db.query("""
                    SELECT id
                    FROM carshare_lots
                    WHERE city = '{city}'
                        AND name = '{name}'
                """.format(city=city, name=x["address"]))
                lot_id = lot[0][0] if lot else "NULL"
                slot_id = "NULL"
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
                lot_id = "NULL"

            # update or insert
            if x["vin"] in our_vins and not x["vin"] in parked_vins:
                query = update_car2go.format(
                    vin=x["vin"], name=x["name"].encode('utf-8'), long=x["coordinates"][0], lat=x["coordinates"][1],
                    address=x["address"], slot_id=slot_id, lot_id=lot_id, fuel=x["fuel"]
                )
            elif not x["vin"] in our_vins:
                query = insert_car2go.format(
                    city=city, vin=x["vin"], name=x["name"].encode('utf-8'), long=x["coordinates"][0], lat=x["coordinates"][1],
                    address=x["address"], slot_id=slot_id, lot_id=lot_id, fuel=x["fuel"]
                )
            if query:
                queries.append(query)

    db.queries(queries)


def update_automobile():
    """
    Task to check with the Auto-mobile API, find moved cars and update their positions/slots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    queries = []

    insert_autom = """
        INSERT INTO carshares (company, city, vin, name, address, slot_id, parked, fuel, electric, geom, geojson)
            SELECT 'auto-mobile', c.name, '{vin}', '{name}', s.way_name, {slot_id}, true, {fuel},
                    CASE WHEN right('{name}', 2) = '-R' THEN true ELSE false END,
                    ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
                    ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb
            FROM slots s
            JOIN cities c ON ST_Intersects(ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857), c.geom)
            WHERE s.id = {slot_id};
    """

    update_autom = """
        UPDATE carshares SET since = NOW(), name = '{name}', address = s.way_name, slot_id = {slot_id},
            parked = true, fuel = {fuel}, geom = ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
            geojson = ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb
        FROM slots s
        WHERE vin = '{vin}'
            AND s.id = {slot_id}
    """

    # grab data from Auto-mobile api
    data = requests.get("https://www.reservauto.net/WCF/LSI/LSIBookingService.asmx/GetVehicleProposals",
        params={"Longitude": "-73.56307727766432", "Latitude": "45.48420949674474", "CustomerID": '""'})
    data = demjson.decode(data.text.lstrip("(").rstrip(");"))["Vehicules"]

    # unpark stale entries in our database
    our_vins = db.query("SELECT vin FROM carshares WHERE company = 'auto-mobile'")
    our_vins = [x[0] for x in our_vins] if our_vins else []
    parked_vins = db.query("SELECT vin FROM carshares WHERE company = 'auto-mobile' AND parked = true")
    parked_vins = [x[0] for x in parked_vins] if parked_vins else []
    their_vins = [x["Id"] for x in data]
    for x in parked_vins:
        if not x in their_vins:
            queries.append("UPDATE carshares SET since = NOW(), parked = false WHERE vin = '{vin}'".format(vin=x))

    # create or update Auto-mobile tracking with new data
    for x in data:
        query = None
        slot = db.query("""
            SELECT s.id
            FROM slots s
            JOIN cities c ON ST_Intersects(ST_Transform('SRID=4326;POINT({x} {y})'::geometry, 3857), c.geom)
            WHERE s.city = c.name
                AND ST_DWithin(
                    ST_Transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                    s.geom,
                    5
                )
            ORDER BY ST_Distance(st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857), s.geom)
            LIMIT 1
        """.format(x=x["Position"]["Lon"], y=x["Position"]["Lat"]))
        slot_id = slot[0][0] if slot else "NULL"

        # update or insert
        if x["Id"] in our_vins and not x["Id"] in parked_vins:
            query = update_autom.format(
                vin=x["Id"], name=x["Immat"].encode('utf-8'), long=x["Position"]["Lon"],
                lat=x["Position"]["Lat"], slot_id=slot_id, fuel=x["EnergyLevel"]
            )
        elif not x["Id"] in our_vins:
            query = insert_autom.format(
                vin=x["Id"], name=x["Immat"].encode('utf-8'), long=x["Position"]["Lon"],
                lat=x["Position"]["Lat"], slot_id=slot_id, fuel=x["EnergyLevel"]
            )
        if query:
            queries.append(query)

    db.queries(queries)


def update_communauto():
    """
    Task to check with the Communuauto API, find moved cars and update their positions/slots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    queries = []

    insert_comm = """
        INSERT INTO carshares (company, city, partners_id, name, address, lot_id, geom, geojson)
            SELECT 'communauto', '{city}', '{pid}', '{name}', '{address}', {lot_id},
                    ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
                    ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb;
    """

    update_comm = """
        UPDATE carshares SET since = NOW(), name = '{name}', address = '{address}', parked = true,
            geom = ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
            geojson = ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb
        WHERE company = 'communauto'
            AND partners_id = '{pid}'
    """

    insert_lot = """
        INSERT INTO carshare_lots (company, city, name, capacity, available, partners_id, geom, geojson)
            SELECT 'communauto', '{city}', '{name}', 1, {available}, {pid},
                    ST_Transform('SRID=4326;POINT({long} {lat})'::geometry, 3857),
                    ST_AsGeoJSON('SRID=4326;POINT({long} {lat})'::geometry)::jsonb;
    """

    update_lot = """
        UPDATE carshare_lots SET capacity = 1, available = {available}
        WHERE city = '{city}' AND partners_id = '{pid}'
    """

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

        our_lots = db.query("SELECT partners_id FROM carshare_lots WHERE company = 'communauto' AND city = '{city}'"\
            .format(city=city))
        our_lots = [x[0] for x in our_lots] if our_lots else []
        for x in data:
            if x["StationID"] in our_lots:
                queries.append(update_lot.format(city=city, pid=x["StationID"], available=(1 if x["NbrRes"] else 0)))
            else:
                queries.append(insert_lot.format(city=city, pid=x["StationID"], name=x["strNomStation"].replace("'", "''").encode("utf-8"),
                    available=(1 if x["NbrRes"] else 0), long=x["Longitude"], lat=x["Latitude"]))
        db.queries(queries)
        queries = []

        # unpark stale entries in our database
        our_pids = db.query("SELECT partners_id FROM carshares WHERE company = 'communauto' AND city = '{city}'".format(city=city))
        our_pids = [x[0] for x in our_pids] if our_pids else []
        parked_pids = db.query("SELECT partners_id FROM carshares WHERE company = 'communauto' AND city = '{city}' AND parked = true".format(city=city))
        parked_pids = [x[0] for x in parked_pids] if parked_pids else []
        for x in data:
            if x["NbrRes"] > 0 and x["CarID"] in parked_pids:
                queries.append("UPDATE carshares SET since = NOW(), parked = false WHERE company = 'communauto'"
                    " AND city = '{city}' AND partners_id = '{pid}'".format(city=city, pid=x["CarID"]))

        # create or update communauto tracking with new data
        for x in data:
            query = None
            x["strNomStation"] = x["strNomStation"].replace("'", "''").encode("utf-8")

            lot = db.query("""
                SELECT id
                FROM carshare_lots
                WHERE company = 'communauto'
                    AND city = '{city}'
                    AND partners_id = '{pid}'
            """.format(city=city, pid=x["StationID"]))
            lot_id = lot[0][0]

            # update or insert
            if x["CarID"] in our_pids and not x["CarID"] in parked_pids:
                query = update_comm.format(
                    pid=x["CarID"], name=x["Model"].encode('utf-8'), long=x["Longitude"], lat=x["Latitude"],
                    address=x["strNomStation"], lot_id=lot_id
                )
            elif not x["CarID"] in our_pids:
                query = insert_comm.format(
                    city=city, pid=x["CarID"], name=x["Model"].encode('utf-8'), long=x["Longitude"],
                    lat=x["Latitude"], address=x["strNomStation"], lot_id=lot_id
                )
            if query:
                queries.append(query)

    db.queries(queries)


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
