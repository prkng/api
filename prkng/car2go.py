# -*- coding: utf-8 -*-
import json
import os
import urllib2

from prkng import create_app
from prkng.admin import auth_required, create_token
from prkng.models import Car2Go
from prkng.database import PostgresWrapper

from flask import current_app, jsonify, Blueprint, request, send_from_directory


car2go = Blueprint('car2go', __name__, url_prefix='/car2go')


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
  parked boolean DEFAULT true,
  in_lot boolean DEFAULT false
)
"""

insert_car2go = """
INSERT INTO car2go (vin, name, long, lat, address, slot_id, in_lot)
    SELECT '{vin}', '{name}', {long}, {lat}, '{address}', {slot_id}, {in_lot};
"""

update_car2go = """
UPDATE car2go SET since = NOW(), name = '{name}', long = {long}, lat = {lat}, address = '{address}',
        slot_id = {slot_id}, in_lot = {in_lot}, parked = true
    WHERE vin = '{vin}'
"""


def init_car2go(app):
    """
    Initialize login manager extension into flask application
    """
    app.register_blueprint(car2go)


@car2go.route('/', defaults={'path': None})
@car2go.route('/<path:path>')
def test_view(path):
    """
    Serve car2go interface.
    Should only be used for testing; otherwise serve with NGINX instead.
    """
    if path and not path.startswith(("assets", "public", "fonts", "images")):
        path = None
    sdir = os.path.dirname(os.path.realpath(__file__))
    if path and path.startswith("images"):
        sdir = os.path.abspath(os.path.join(sdir, '../../prkng-car2go/public'))
    else:
        sdir = os.path.abspath(os.path.join(sdir, '../../prkng-car2go/dist'))
    return send_from_directory(sdir, path or 'index.html')


@car2go.route('/api/token', methods=['POST'])
def generate_token():
    """
    Generate a JSON Web Token for use with Ember.js admin
    """
    data = json.loads(request.data)
    uname, passwd = data.get("username"), data.get("password")
    if uname in current_app.config["CAR2GO_ACCTS"] \
    and passwd == current_app.config["CAR2GO_ACCTS"][uname]:
        return jsonify(token=create_token(uname))
    else:
        return jsonify(message="Username or password incorrect"), 401


@car2go.route('/api/cars', methods=['GET'])
@auth_required()
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
    raw = urllib2.urlopen("http://www.car2go.com/api/v2.1/vehicles?loc=montreal&format=json&oauth_consumer_key=%s" % CONFIG["CAR2GO_CONSUMER"])
    data = json.loads(raw.read())["placemarks"]

    raw = urllib2.urlopen("http://www.car2go.com/api/v2.1/parkingspots?loc=montreal&format=json&oauth_consumer_key=%s" % CONFIG["CAR2GO_CONSUMER"])
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
            queries.append("UPDATE car2go SET parked = false WHERE vin = '{}'".format(x))

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
