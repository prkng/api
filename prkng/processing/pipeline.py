# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from __future__ import unicode_literals

import csv
import json
import os

from prkng.logger import Logger
from prkng import create_app
from prkng.database import PostgresWrapper

import osm
import montreal as mrl
import quebec as qbc
import newyork as nyc
import plfunctions
import common
from .filters import group_rules


CONFIG = create_app().config
db = PostgresWrapper(
    "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
    "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

# distance from road to slot
LINE_OFFSET = 6

CITIES = ["montreal", "quebec", "newyork"]


def process_quebec():
    """
    Process Quebec data
    """
    def info(msg):
        return Logger.info("Québec: {}".format(msg))

    def debug(msg):
        return Logger.debug("Québec: {}".format(msg))

    def warning(msg):
        return Logger.warning("Québec: {}".format(msg))

    info('Loading and translating rules')
    insert_rules('quebec_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Creating sign table")
    db.query(qbc.create_sign)

    info("Loading signs")
    db.query(qbc.insert_sign)
    db.create_index('quebec_sign', 'direction')
    db.create_index('quebec_sign', 'code')
    db.create_index('quebec_sign', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'quebec_sign')

    info("Creating signposts")
    db.query(qbc.create_signpost)
    db.create_index('quebec_signpost', 'id')
    db.create_index('quebec_signpost', 'rid')
    db.create_index('quebec_signpost', 'signs', index_type='gin')
    db.create_index('quebec_signpost', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'quebec_signpost')

    info("Add signpost id to signs")
    db.query(qbc.add_signposts_to_sign)
    db.vacuum_analyze('public', 'quebec_sign')

    info("Projection signposts on road")
    duplicates = db.query(qbc.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    percent, total = db.query(qbc.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(qbc.generate_signposts_orphans)
        info("Table 'signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(qbc.create_slots_likely)
    db.query(qbc.insert_slots_likely.format(isleft=1))
    db.query(qbc.insert_slots_likely.format(isleft=-1))
    db.create_index('quebec_slots_likely', 'id')
    db.create_index('quebec_slots_likely', 'signposts', index_type='gin')
    db.create_index('quebec_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'quebec_slots_likely')

    db.query(qbc.create_nextpoints_for_signposts)
    db.create_index('quebec_nextpoints', 'id')
    db.create_index('quebec_nextpoints', 'slot_id')
    db.create_index('quebec_nextpoints', 'direction')
    db.vacuum_analyze('public', 'quebec_nextpoints')

    db.query(qbc.insert_slots_temp.format(offset=LINE_OFFSET))
    db.create_index('quebec_slots_temp', 'id')
    db.create_index('quebec_slots_temp', 'geom', index_type='gist')
    db.create_index('quebec_slots_temp', 'rules', index_type='gin')
    db.vacuum_analyze('public', 'quebec_slots_temp')

    info("Creating and overlaying paid slots")
    db.query(qbc.create_bornes_raw)
    db.query(qbc.create_paid_signpost)
    db.query(qbc.aggregate_paid_signposts.format(offset=LINE_OFFSET))
    db.query(qbc.overlay_paid_rules)
    db.query(qbc.create_paid_slots_standalone)

    if CONFIG['DEBUG']:
        db.query(qbc.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('quebec_slots_debug', 'pkid')
        db.create_index('quebec_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'quebec_slots_debug')


def process_montreal():
    """
    process montreal data and generate parking slots
    """
    def info(msg):
        return Logger.info("Montréal: {}".format(msg))

    def debug(msg):
        return Logger.debug("Montréal: {}".format(msg))

    def warning(msg):
        return Logger.warning("Montréal: {}".format(msg))

    debug('Loading and translating rules')
    insert_rules('montreal_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Creating sign table")
    db.query(mrl.create_sign)

    info("Loading signs")
    db.query(mrl.insert_sign)
    db.create_index('montreal_sign', 'geom', index_type='gist')
    db.create_index('montreal_sign', 'direction')
    db.create_index('montreal_sign', 'elevation')
    db.create_index('montreal_sign', 'signpost')
    db.vacuum_analyze('public', 'montreal_sign')

    info("Creating sign posts")
    db.query(mrl.create_signpost)
    db.query(mrl.insert_signpost)
    db.create_index('montreal_signpost', 'geom', index_type='gist')
    db.create_index('montreal_signpost', 'geobase_id')
    db.vacuum_analyze('public', 'montreal_signpost')

    info("Matching osm roads with geobase")
    db.query(mrl.match_roads_geobase)
    db.create_index('montreal_roads_geobase', 'id')
    db.create_index('montreal_roads_geobase', 'id_trc')
    db.create_index('montreal_roads_geobase', 'osm_id')
    db.create_index('montreal_roads_geobase', 'name')
    db.create_index('montreal_roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'montreal_roads_geobase')

    info("Projecting signposts on road")
    duplicates = db.query(mrl.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    db.create_index('montreal_signpost_onroad', 'id')
    db.create_index('montreal_signpost_onroad', 'road_id')
    db.create_index('montreal_signpost_onroad', 'isleft')
    db.create_index('montreal_signpost_onroad', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'montreal_signpost_onroad')

    percent, total = db.query(mrl.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(mrl.generate_signposts_orphans)
        info("Table 'montreal_signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(mrl.create_slots_likely)
    db.query(mrl.insert_slots_likely.format(isleft=1))
    db.query(mrl.insert_slots_likely.format(isleft=-1))
    db.create_index('montreal_slots_likely', 'id')
    db.create_index('montreal_slots_likely', 'signposts', index_type='gin')
    db.create_index('montreal_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'montreal_slots_likely')

    db.query(mrl.create_nextpoints_for_signposts)
    db.create_index('montreal_nextpoints', 'id')
    db.create_index('montreal_nextpoints', 'slot_id')
    db.create_index('montreal_nextpoints', 'direction')
    db.vacuum_analyze('public', 'montreal_nextpoints')

    db.create_index('montreal_slots_temp', 'id')
    db.create_index('montreal_slots_temp', 'geom', index_type='gist')
    db.create_index('montreal_slots_temp', 'rules', index_type='gin')
    db.query(mrl.insert_slots_temp.format(offset=LINE_OFFSET))

    info("Creating and overlaying paid slots")
    db.query(mrl.overlay_paid_rules)
    db.vacuum_analyze('public', 'montreal_slots_temp')

    if CONFIG['DEBUG']:
        db.query(mrl.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('montreal_slots_debug', 'pkid')
        db.create_index('montreal_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'montreal_slots_debug')


def process_newyork():
    """
    Process New York data
    """
    def info(msg):
        return Logger.info("New York: {}".format(msg))

    def debug(msg):
        return Logger.debug("New York: {}".format(msg))

    def warning(msg):
        return Logger.warning("New York: {}".format(msg))

    info('Loading and translating rules')
    insert_rules('newyork_rules_translation')
    db.vacuum_analyze('public', 'rules')

    info("Loading signs")
    db.query(nyc.create_sign)
    db.query(nyc.insert_sign)
    db.create_index('newyork_sign', 'direction')
    db.create_index('newyork_sign', 'code')
    db.create_index('newyork_sign', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_sign')

    info("Creating signposts")
    db.query(nyc.create_signpost)
    db.query(nyc.insert_signpost)
    db.create_index('newyork_signpost', 'id')
    db.create_index('newyork_signpost', 'geobase_id')
    db.create_index('newyork_signpost', 'signs', index_type='gin')
    db.create_index('newyork_signpost', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_signpost')

    info("Matching osm roads with geobase")
    db.query(nyc.match_roads_geobase)
    db.create_index('newyork_roads_geobase', 'id')
    db.create_index('newyork_roads_geobase', 'osm_id')
    db.create_index('newyork_roads_geobase', 'name')
    db.create_index('newyork_roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_roads_geobase')

    info("Match signposts to geobase")
    db.query(nyc.match_signposts)
    db.vacuum_analyze('public', 'newyork_signpost')

    info("Add signpost id to signs")
    db.query(nyc.add_signposts_to_sign)
    db.vacuum_analyze('public', 'newyork_sign')

    info("Projecting signposts on road")
    duplicates = db.query(nyc.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    percent, total = db.query(nyc.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(nyc.generate_signposts_orphans)
        info("Table 'newyork_signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(nyc.create_slots_likely)
    db.query(nyc.insert_slots_likely.format(isleft=1))
    db.query(nyc.insert_slots_likely.format(isleft=-1))

    # Get rid of problem segments FIXME
    db.query("""
        with tmp as (
            select *
            from (
                select g.id, count(distinct s.order_no)
                from newyork_roads_geobase g
                join newyork_signpost s on s.geobase_id = g.id
                group by g.id
            ) foo where count > 2
        )
        delete from newyork_slots_likely s using tmp t where t.id = s.rid;
    """)

    db.create_index('newyork_slots_likely', 'id')
    db.create_index('newyork_slots_likely', 'signposts', index_type='gin')
    db.create_index('newyork_slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'newyork_slots_likely')

    db.query(nyc.create_nextpoints_for_signposts)
    db.create_index('newyork_nextpoints', 'id')
    db.create_index('newyork_nextpoints', 'slot_id')
    db.create_index('newyork_nextpoints', 'direction')
    db.vacuum_analyze('public', 'newyork_nextpoints')

    db.query(nyc.insert_slots_temp.format(offset=LINE_OFFSET))
    db.create_index('newyork_slots_temp', 'id')
    db.create_index('newyork_slots_temp', 'geom', index_type='gist')
    db.create_index('newyork_slots_temp', 'rules', index_type='gin')
    db.vacuum_analyze('public', 'newyork_slots_temp')

    if CONFIG['DEBUG']:
        db.query(nyc.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('newyork_slots_debug', 'pkid')
        db.create_index('newyork_slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'newyork_slots_debug')


def cleanup_table():
    """
    Remove temporary tables
    """
    Logger.info("Cleanup schema")

    # drop universal temp tables
    for x in ["bad_intersection", "way_intersection", "roads", "signpost_onroad",
            "permit_zones", "parking_lots_raw"]:
        db.query("DROP TABLE IF EXISTS {}".format(x))

    # drop per-city temp tables
    for x in ["slots_likely", "slots_temp", "nextpoints", "paid_temp", "signpost_temp",
            "paid_slots_raw", "bornes_raw", "bornes_clustered"]:
        for y in CITIES:
            db.query("DROP TABLE IF EXISTS {}_{}".format(y, x))


def process_osm():
    """
    Process OSM data
    """
    def info(msg):
        return Logger.info("OpenStreetMap: {}".format(msg))

    def debug(msg):
        return Logger.debug("OpenStreetMap: {}".format(msg))

    def warning(msg):
        return Logger.warning("OpenStreetMap: {}".format(msg))

    info("Filtering ways")
    db.query(osm.create_osm_ways)
    db.create_index('osm_ways', 'geom', index_type='gist')
    db.create_index('osm_ways', 'osm_id')
    db.create_index('osm_ways', 'name')

    info("Creating way intersections from planet lines")
    db.query(osm.create_way_intersection)
    db.create_index('way_intersection', 'way_id')
    db.create_index('way_intersection', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'way_intersection')
    res = db.query(osm.remove_bad_intersection)
    if res:
        debug("Removed {} bad intersections".format(len(res)))

    info("Splitting ways on intersections")
    db.query(osm.split_osm_roads)
    db.create_index('roads', 'id')
    db.create_index('roads', 'osm_id')
    db.create_index('roads', 'name')
    db.create_index('roads', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'roads')


def run(cities=CITIES, osm=False):
    """
    Run the entire pipeline
    """
    Logger.debug("Loading extensions and custom functions")
    db.query("create extension if not exists fuzzystrmatch")
    db.query("create extension if not exists intarray")
    db.query(plfunctions.st_isleft_func)
    db.query(plfunctions.array_sort)
    db.query(plfunctions.get_max_range)

    if osm:
        process_osm()

    # create common tables
    db.query(common.create_rules)
    db.create_index('rules', 'code')
    db.query(common.create_slots)

    for x in cities:
        db.query(common.create_slots_temp.format(city=x))
        db.query(common.create_slots_partition.format(city=x))
        db.query(common.create_corrections.format(city=x))

    Logger.info("Processing parking lot / garage data")
    db.query(common.create_parking_lots)
    db.query(common.create_parking_lots_raw.format(city="montreal"))
    insert_raw_lots("montreal", "lots_montreal.csv")
    insert_parking_lots("montreal")
    db.query(common.create_parking_lots_raw.format(city="quebec"))
    insert_raw_lots("quebec", "lots_quebec.csv")
    insert_parking_lots("quebec")
    db.create_index('parking_lots', 'id')
    db.create_index('parking_lots', 'city')
    db.create_index('parking_lots', 'geom', index_type='gist')
    db.create_index('parking_lots', 'agenda', index_type='gin')

    if 'montreal' in cities:
        process_montreal()
    if 'quebec' in cities:
        process_quebec()
    if 'newyork' in cities:
        process_newyork()

    Logger.info("Shorten slots that intersect with roads or other slots")
    for x in cities:
        db.query(common.cut_slots_crossing_roads.format(city=x, offset=LINE_OFFSET))
        db.query(common.cut_slots_crossing_slots.format(city=x))

    Logger.info("Aggregating like slots")
    for x in cities:
        db.create_index(x+'_slots', 'id')
        db.create_index(x+'_slots', 'geom', index_type='gist')
        db.create_index(x+'_slots', 'rules', index_type='gin')
        db.query(common.aggregate_like_slots.format(city=x))
        db.query(common.create_client_data.format(city=x))
        db.vacuum_analyze('public', x+'_slots')

    Logger.info("Mapping corrections to new slots")
    db.query(common.process_corrected_rules)
    for x in cities:
        db.query(common.process_corrections.format(city=x))

    if not CONFIG['DEBUG']:
        cleanup_table()


def insert_rules(from_table):
    """
    Get rules from specific location (montreal, quebec),
    group them, make a simpler model and load them into database
    """
    Logger.debug("Get rules from {} and simplify them".format(from_table))
    rules = db.query(
        common.get_rules_from_source.format(source=from_table),
        namedtuple=True
    )
    rules_grouped = group_rules(rules)

    Logger.debug("Load rules into rules table")

    db.copy_from('public', 'rules', common.rules_columns, [
        [
            json.dumps(val).replace('\\', '\\\\') if isinstance(val, dict) else val
            for val in rule._asdict().values()]
        for rule in rules_grouped
    ])


def insert_raw_lots(city, filename):
    db.query("""
        COPY {}_parking_lots (name, operator, address, description, lun_normal, mar_normal, mer_normal,
            jeu_normal, ven_normal, sam_normal, dim_normal, hourly_normal, daily_normal, max_normal,
            lun_special, mar_special, mer_special, jeu_special, ven_special, sam_special, dim_special,
            hourly_special, daily_special, max_special, lun_free, mar_free, mer_free, jeu_free,
            ven_free, sam_free, dim_free, daily_free, indoor, handicap, card, valet, lat, long,
            capacity, street_view_lat, street_view_long, street_view_head, street_view_id, active)
        FROM '{}'
        WITH CSV HEADER
    """.format(city, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', filename)))


def insert_parking_lots(city):
    columns = ["city", "name", "operator", "address", "description", "agenda", "capacity", "attrs", "geom", "active", "street_view", "geojson"]
    days = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]
    lots, queries = [], []

    for row in db.query("""
        SELECT *, ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3857) AS geom
        FROM {}_parking_lots
    """.format(city), namedtuple=True):
        lot = [(x.decode('utf-8').replace("'", "''") if x else '') for x in [row.name, row.operator, row.address, row.description]]
        agenda = {}

        # Create pricing rules per time period the lot is open
        for x in range(1,8):
            agenda[str(x)] = []
            if getattr(row, days[x - 1] + "_normal"):
                y = getattr(row, days[x - 1] + "_normal")
                agenda[str(x)].append({"hours": [float(z) for z in y.split(",")],
                    "hourly": row.hourly_normal or None, "max": row.max_normal or None,
                    "daily": row.daily_normal or None})
            if getattr(row, days[x - 1] + "_special"):
                y = getattr(row, days[x - 1] + "_special")
                agenda[str(x)].append({"hours": [float(z) for z in y.split(",")],
                    "hourly": row.hourly_normal or None, "max": row.max_normal or None,
                    "daily": row.daily_normal or None})
            if getattr(row, days[x - 1] + "_free"):
                y = getattr(row, days[x - 1] + "_free")
                agenda[str(x)].append({"hours": [float(z) for z in y.split(",")],
                    "hourly": 0, "max": None, "daily": row.daily_free or None})

        # Create "closed" rules for periods not covered by an open rule
        for x in agenda:
            hours = sorted([y["hours"] for y in agenda[x]], key=lambda z: z[0])
            for i, y in enumerate(hours):
                starts = [z[0] for z in hours]
                if y[0] == 0.0:
                    continue
                last_end = hours[i-1][1] if not i == 0 else 0.0
                next_start = hours[i+1][0] if not i == (len(hours) - 1) else 24.0
                if not last_end in starts:
                    agenda[x].append({"hours": [last_end, y[0]], "hourly": None, "max": None,
                        "daily": None})
                if not next_start in starts and y[1] != 24.0:
                    agenda[x].append({"hours": [y[1], next_start], "hourly": None, "max": None,
                        "daily": None})
            if agenda[x] == []:
                agenda[x].append({"hours": [0.0,24.0], "hourly": None, "max": None, "daily": None})

        lot += [json.dumps(agenda), row.capacity or 0, json.dumps({"indoor": row.indoor,
            "handicap": row.handicap, "card": row.card, "valet": row.valet}), row.geom, row.active,
            row.street_view_head, row.street_view_id]
        lots.append(lot)

    for x in lots:
        queries.append("""
            INSERT INTO parking_lots ({}) VALUES ('{city}', '{}', '{}', '{}', '{}', '{}'::jsonb, {},
                '{}'::jsonb, '{}'::geometry, '{}', json_build_object('head', {}, 'id', '{}')::jsonb,
                ST_AsGeoJSON(ST_Transform('{geom}'::geometry, 4326))::jsonb)
        """.format(",".join(columns), *[y for y in x], city=city, geom=x[-4]))
    db.queries(queries)
