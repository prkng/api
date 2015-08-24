from prkng.database import db, metadata

import datetime
import json
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Table, text
import urllib2


car2go_table = Table(
    'car2go',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('slot_id', Integer),
    Column('vin', String, unique=True),
    Column('name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('address', String),
    Column('since', DateTime, server_default=text('NOW()')),
    Column('parked', Boolean),
    Column('in_lot', Boolean)
)


class Car2Go(object):
    @staticmethod
    def get(name):
        """
        Get a car2go by its name.
        """
        res = db.engine.execute("SELECT * FROM car2go WHERE name = {}".format(name)).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def get_all():
        """
        Get all active car2go records.
        """
        res = db.engine.execute("""
            SELECT
                c.id,
                c.name,
                c.vin,
                c.address,
                to_char(c.since, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS since,
                c.long,
                c.lat,
                s.rules
            FROM car2go c
            JOIN slots s ON c.slot_id = s.id
            WHERE c.in_lot = false
                AND c.parked = true
        """).fetchall()
        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

    def update():
        # grab data from car2go api
        raw = urllib2.urlopen("https://www.car2go.com/api/v2.1/vehicles?loc=montreal&format=json&oauth_consumer_key=%s" % CONFIG["CAR2GO_CONSUMER"])
        data = json.loads(raw.read())["placemarks"]

        raw = urllib2.urlopen("https://www.car2go.com/api/v2.1/parkingspots?loc=montreal&format=json&oauth_consumer_key=%s" % CONFIG["CAR2GO_CONSUMER"])
        lot_data = json.loads(raw.read())["placemarks"]
        lots = [x["name"] for x in lot_data]

        # unpark stale entries in our database
        our_vins = car2go_table.select([car2go_table.c.vin])
        parked_vins = car2go_table.select([car2go_table.c.vin]).where(car2go_table.c.parked == True)
        their_vins = [x["vin"] for x in data]
        for x in parked_vins:
            if not x in their_vins:
                car2go_table.update().where(car2go_table.c.vin == x).values(since=text('NOW()'), parked=False)

        # create or update car2go tracking with new data
        for x in data:
            # if the address matches a car2go reserved lot, don't bother with a slot
            if x["address"] in lots:
                slot_id = "NULL"
                in_lot = True
            # otherwise grab the most likely slot within 5m
            else:
                slot = db.engine.execute("""
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
                car2go_table.update().where(car2go_table.c.vin == x["vin"]).values(since=text('NOW()'),
                    name=x["name"], long=x["coordinates"][0], lat=x["coordinates"][1],
                    addres=x["address"].replace("'", "''").encode('utf-8'), slot_id=slot_id,
                    in_lot=in_lot, parked=True)
            elif not x["vin"] in our_vins:
                car2go_table.insert().values(vin=x["vin"], name=x["name"], long=x["coordinates"][0],
                    lat=x["coordinates"][1], address=x["address"].replace("'", "''").encode('utf-8'),
                    slot_id=slot_id, in_lot=in_lot)
