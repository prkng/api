# -*- coding: utf-8 -*-

from prkng import create_app, notifications
from prkng.database import PostgresWrapper

import datetime
import demjson
import pytz
from redis import Redis
import requests


def update_car2go():
    """
    Task to check with the car2go API, find moved cars and update their positions/slots
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    for city in ["montreal", "newyork", "seattle"]:
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
            x.get("fuel", 0), x["coordinates"][0], x["coordinates"][1]) for x in data]
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
    if data:
        db.query("""
            UPDATE carshares c SET since = NOW(), parked = false
            WHERE c.company = 'auto-mobile'
                AND c.parked = true
                AND (SELECT 1 FROM (VALUES {data}) AS d(pid) WHERE c.vin = d.pid LIMIT 1) IS NULL
        """.format(data=",".join(["('{}')".format(x["Id"]) for x in data])))

        # create or update Auto-mobile tracking with newly parked vehicles
        values = ["('{}','{}',{},{},'{}','SRID=4326;POINT({} {})'::geometry)".format(x["Id"],
            x["Immat"].encode('utf-8'), x["EnergyLevel"], ("true" if x["Name"].endswith("-R") else "false"),
            x["Name"].encode('utf-8'), x["Position"]["Lon"], x["Position"]["Lat"]) for x in data]
        db.query("""
            WITH tmp AS (
                SELECT DISTINCT ON (d.vin) d.vin, d.name, d.fuel, d.id, s.id AS slot_id, s.way_name, d.geom
                FROM (VALUES {}) AS d(vin, name, fuel, electric, id, geom)
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
        start = datetime.datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
        finish = (start + datetime.timedelta(minutes=30))
        data = requests.post("https://www.reservauto.net/Scripts/Client/Ajax/PublicCall/Get_Car_DisponibilityJSON.asp",
            data={"CityID": cacity, "StartDate": start.strftime("%d/%m/%Y %H:%M"),
                "EndDate": finish.strftime("%d/%m/%Y %H:%M"), "FeeType": 80})
        # must use demjson here because returning format is non-standard JSON
        try:
            data = demjson.decode(data.text.lstrip("(").rstrip(")"))["data"]
        except:
            return

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

        values = ["('{}','{}','{}','{}','{}',{},'{}'::timestamp,'SRID=4326;POINT({} {})'::geometry)".format(city,
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


def update_zipcar():
    """
    Task to check with the Zipcar API and update parking lot data
    """
    CONFIG = create_app().config
    db = PostgresWrapper(
        "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
        "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    lots, cars, vids = [], [], []
    raw = requests.get("https://api.zipcar.com/partner-api/directory",
        params={"country": "us", "embed": "vehicles", "apikey": CONFIG["ZIPCAR_KEY"]})
    data = raw.json()["locations"]
    for x in data:
        if not x["address"]["city"] or not x["address"]["city"] \
                in ["Seattle", "New York", "Brooklyn", "Queens", "Staten Island"]:
            continue
        city = x["address"]["city"].encode("utf-8").lower()
        if x["address"]["city"] in ["New York", "Brooklyn", "Queens", "Staten Island"]:
            city = "newyork"
        lots.append("('{}','{}','{}',{},'SRID=4326;POINT({} {})'::geometry)".format(
            x["location_id"], city, x["display_name"].replace("'", "''").encode("utf-8"),
            len(x["vehicles"]), x["coordinates"]["lng"], x["coordinates"]["lat"]
        ))
        for y in x["vehicles"]:
            cars.append("('{}','{}','{}','{}','{}','SRID=4326;POINT({} {})'::geometry)".format(
                y["vehicle_id"], y["vehicle_name"].replace("'", "''").encode("utf-8"),
                city, x["address"]["street"].replace("'", "''").encode("utf-8"),
                x["location_id"], x["coordinates"]["lng"], x["coordinates"]["lat"]
            ))
            vids.append(y["vehicle_id"])

    if lots:
        db.query("""
            UPDATE carshare_lots l SET name = d.name, capacity = d.capacity, available = d.capacity
            FROM (VALUES {}) AS d(pid, city, name, capacity, geom)
            WHERE l.company = 'zipcar'
                AND l.partner_id = d.pid
                AND (l.available != d.capacity OR l.capacity != d.capacity OR l.name != d.name)
        """.format(",".join(lots)))
        db.query("""
            INSERT INTO carshare_lots (company, partner_id, city, name, capacity, available, geom, geojson)
            SELECT 'zipcar', d.pid, d.city, d.name, d.capacity, d.capacity,
                    ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
            FROM (VALUES {}) AS d(pid, city, name, capacity, geom)
            WHERE (SELECT 1 FROM carshare_lots l WHERE l.city = d.city AND l.partner_id = d.pid LIMIT 1) IS NULL
        """.format(",".join(lots)))
    if cars:
        db.query("""
            INSERT INTO carshares (company, city, partner_id, name, address, lot_id, parked, geom, geojson)
                SELECT 'zipcar', d.city, d.pid, d.name, d.address, l.id, true,
                        ST_Transform(d.geom, 3857), ST_AsGeoJSON(d.geom)::jsonb
                FROM (VALUES {}) AS d(pid, name, city, address, lot_pid, geom)
                JOIN carshare_lots l ON l.company = 'zipcar' AND l.city = d.city
                    AND l.partner_id = d.lot_pid
                WHERE (SELECT 1 FROM carshares c WHERE c.partner_id = d.pid LIMIT 1) IS NULL
        """.format(",".join(cars)))
    db.query("""
        DELETE FROM carshare_lots l
        WHERE l.company = 'zipcar'
            AND (SELECT 1 FROM (VALUES {}) AS d(pid) WHERE l.company = 'zipcar' AND l.partner_id = d.pid) IS NULL
    """.format(",".join(["('{}')".format(z["location_id"]) for z in data])))
    db.query("""
        DELETE FROM carshares l
        WHERE l.company = 'zipcar'
            AND (SELECT 1 FROM (VALUES {}) AS d(pid) WHERE l.company = 'zipcar' AND l.partner_id = d.pid) IS NULL
    """.format(",".join(["('{}')".format(z) for z in vids])))


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
