from prkng import create_app
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
    scheduler.schedule(scheduled_time=now, func=update_analytics, interval=120, result_ttl=240, repeat=None)
    scheduler.schedule(scheduled_time=now, func=update_free_spaces, interval=300, result_ttl=600, repeat=None)
    if not debug:
        scheduler.schedule(scheduled_time=datetime.datetime.combine(datetime.date.today() + datetime.timedelta(days=1), datetime.time(8)),
            func=run_backup, args=["prkng", "prkng"], interval=86400, result_ttl=172800, repeat=None)

def stop_tasks():
    for x in scheduler.get_jobs():
        scheduler.cancel(x)

def run_backup(username, database):
    file_name = 'prkng-{}.sql.gz'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
    if not os.path.exists("/backup"):
        os.mkdir("/backup")
    check_call('pg_dump -c -U {PG_USERNAME} {PG_DATABASE} | gzip > {}'.format(os.path.join("/backup", file_name),
        PG_USERNAME=username, PG_DATABASE=database),
        shell=True)
    return os.path.join("/backup", file_name)

def update_car2go():
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
    queries = []

    insert_car2go = """
        INSERT INTO car2go (vin, name, long, lat, address, slot_id, in_lot)
            SELECT '{vin}', '{name}', {long}, {lat}, '{address}', {slot_id}, {in_lot};
    """

    update_car2go = """
        UPDATE car2go SET since = NOW(), name = '{name}', long = {long}, lat = {lat}, address = '{address}',
            slot_id = {slot_id}, in_lot = {in_lot}, parked = true
        WHERE vin = '{vin}'
    """

    # grab data from car2go api
    raw = urllib2.urlopen("https://www.car2go.com/api/v2.1/vehicles?loc=montreal&format=json&oauth_consumer_key=%s" % CONFIG["CAR2GO_CONSUMER"])
    data = json.loads(raw.read())["placemarks"]

    raw = urllib2.urlopen("https://www.car2go.com/api/v2.1/parkingspots?loc=montreal&format=json&oauth_consumer_key=%s" % CONFIG["CAR2GO_CONSUMER"])
    lot_data = json.loads(raw.read())["placemarks"]
    lots = [x["name"] for x in lot_data]

    # unpark stale entries in our database
    our_vins = db.query("SELECT vin FROM car2go")
    our_vins = [x[0] for x in our_vins]
    parked_vins = db.query("SELECT vin FROM car2go WHERE parked = true")
    parked_vins = [x[0] for x in parked_vins]
    their_vins = [x["vin"] for x in data]
    for x in parked_vins:
        if not x in their_vins:
            queries.append("UPDATE car2go SET since = NOW(), parked = false WHERE vin = '{}'".format(x))

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
                WHERE ST_Dwithin(
                    st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                    geom,
                    5
                )
                ORDER BY ST_Distance(st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857), geom)
                LIMIT 1
            """.format(x=x["coordinates"][0], y=x["coordinates"][1]))
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
                vin=x["vin"], name=x["name"], long=x["coordinates"][0], lat=x["coordinates"][1],
                address=x["address"].replace("'", "''").encode('utf-8'), slot_id=slot_id,
                in_lot=in_lot
            )
        if query:
            queries.append(query)

    db.queries(queries)


def update_free_spaces():
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

    db.queries(queries)
