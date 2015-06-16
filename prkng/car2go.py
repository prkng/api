# -*- coding: utf-8 -*-
import json
import urllib2

from prkng import create_app
from prkng.models import Car2Go
from prkng.database import PostgresWrapper

from flask import jsonify, Blueprint, request


def add_cors_to_response(resp):
    resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin','*')
    resp.headers['Access-Control-Allow-Credentials'] = 'true'
    resp.headers['Access-Control-Allow-Methods'] = 'PATCH, PUT, POST, OPTIONS, GET, DELETE'
    resp.headers['Access-Control-Allow-Headers'] = 'Authorization, Origin, X-Requested-With, Accept, DNT, Cache-Control, Accept-Encoding, Content-Type'
    return resp


car2go = Blueprint('car2go', __name__, url_prefix='/car2go')
car2go.after_request(add_cors_to_response)


create_car2go_table = """
CREATE TABLE IF NOT EXISTS car2go
(
  id serial PRIMARY KEY,
  since timestamp DEFAULT NOW(),
  vin varchar UNIQUE,
  name varchar,
  long float,
  lat float,
  address varchar,
  slot_id integer,
  in_lot boolean DEFAULT false
)
"""

insert_car2go = """
INSERT INTO car2go (vin, name, long, lat, address, slot_id, in_lot)
    SELECT '{vin}', '{name}', {long}, {lat}, '{address}', {slot_id}, {in_lot};
"""

update_car2go = """
UPDATE car2go SET name = '{name}', long = {long}, lat = {lat}, address = '{address}',
        slot_id = {slot_id}, in_lot = {in_lot}
    WHERE vin = '{vin}'
"""


def init_car2go(app):
    """
    Initialize login manager extension into flask application
    """
    app.register_blueprint(car2go)


@car2go.route('/api/cars', methods=['GET'])
def get_checkins():
    """
    Get all car2go checkins
    """
    cars = Car2Go.get_all()
    return jsonify(cars=cars), 200


def update():
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    db.query(create_car2go_table)
    queries = []

    # grab data from car2go api
    raw = urllib2.urlopen("http://www.car2go.com/api/v2.0/vehicles?loc=montreal&format=json")
    data = json.loads(raw.read())["placemarks"]

    raw = urllib2.urlopen("http://www.car2go.com/api/v2.0/parkingspots?loc=montreal&format=json")
    lot_data = json.loads(raw.read())["placemarks"]
    lots = [x["name"] for x in lot_data]

    # remove stale entries in our database
    our_vins = db.query("SELECT vin FROM car2go")
    our_vins = [x[0] for x in our_vins]
    their_vins = [x["vin"] for x in data]
    for x in our_vins:
        if not x[0] in their_vins:
            queries.append("DELETE FROM car2go WHERE vin = '{}'".format(x[0]))

    # create or update car2go tracking with new data
    for x in data:
        x["coordinates"] = json.loads(x["coordinates"])

        # if the address matches a car2go reserved lot, don't bother with a slot
        if x["address"] in lots:
            slot_id = "NULL"
            in_lot = True
        # otherwise grab the most likely slot within 3m
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
        if x["vin"] in our_vins:
            query = update_car2go.format(
                vin=x["vin"], name=x["name"], long=x["coordinates"][0], lat=x["coordinates"][1],
                address=x["address"].replace("'", "''").encode('utf-8'), slot_id=slot_id,
                in_lot=in_lot
            )
        else:
            query = insert_car2go.format(
                vin=x["vin"], name=x["name"], long=x["coordinates"][0], lat=x["coordinates"][1],
                address=x["address"].replace("'", "''").encode('utf-8'), slot_id=slot_id,
                in_lot=in_lot
            )
        queries.append(query)

    db.queries(queries)
