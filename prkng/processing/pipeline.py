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
import plfunctions
import common
from .filters import group_rules


CONFIG = create_app().config
db = PostgresWrapper(
    "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
    "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

# distance from road to slot
LINE_OFFSET = 6


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
    db.vacuum_analyze('public', 'sign')

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
    db.create_index('slots_temp', 'id')
    db.create_index('slots_temp', 'geom', index_type='gist')
    db.create_index('slots_temp', 'rules', index_type='gin')
    db.vacuum_analyze('public', 'slots_temp')

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
    db.create_index('sign', 'geom', index_type='gist')
    db.create_index('sign', 'direction')
    db.create_index('sign', 'elevation')
    db.create_index('sign', 'signpost')
    db.vacuum_analyze('public', 'sign')

    info("Creating sign posts")
    db.query(mrl.create_signpost)
    db.query(mrl.insert_signpost)
    db.create_index('signpost', 'geom', index_type='gist')
    db.create_index('signpost', 'geobase_id')
    db.vacuum_analyze('public', 'signpost')

    info("Matching osm roads with geobase")
    db.query(mrl.match_roads_geobase)
    db.create_index('roads_geobase', 'id')
    db.create_index('roads_geobase', 'id_trc')
    db.create_index('roads_geobase', 'osm_id')
    db.create_index('roads_geobase', 'name')
    db.create_index('roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'roads_geobase')

    info("Projecting signposts on road")
    duplicates = db.query(mrl.project_signposts)
    if duplicates:
        warning("Duplicates found for projected signposts : {}"
                .format(str(duplicates)))

    db.create_index('signpost_onroad', 'id')
    db.create_index('signpost_onroad', 'road_id')
    db.create_index('signpost_onroad', 'isleft')
    db.create_index('signpost_onroad', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'signpost_onroad')

    percent, total = db.query(mrl.count_signpost_projected)[0]

    if percent < 100:
        warning("Only {:.0f}% of signposts have been bound to a road. Total is {}"
                .format(percent, total))
        db.query(mrl.generate_signposts_orphans)
        info("Table 'signpost_orphans' has been generated to check for orphans")

    info("Creating slots between signposts")
    db.query(mrl.create_slots_likely)
    db.query(mrl.insert_slots_likely.format(isleft=1))
    db.query(mrl.insert_slots_likely.format(isleft=-1))
    db.create_index('slots_likely', 'id')
    db.create_index('slots_likely', 'signposts', index_type='gin')
    db.create_index('slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'slots_likely')

    db.query(mrl.create_nextpoints_for_signposts)
    db.create_index('nextpoints', 'id')
    db.create_index('nextpoints', 'slot_id')
    db.create_index('nextpoints', 'direction')
    db.vacuum_analyze('public', 'nextpoints')

    db.query(mrl.insert_slots_temp.format(offset=LINE_OFFSET))

    if CONFIG['DEBUG']:
        db.query(mrl.create_slots_for_debug.format(offset=LINE_OFFSET))
        db.create_index('slots_debug', 'pkid')
        db.create_index('slots_debug', 'geom', index_type='gist')
        db.vacuum_analyze('public', 'slots_debug')

    info("Overlaying paid slots")
    db.query(mrl.create_paid_temp)
    db.query("""
        COPY montreal_paid_temp (signposts)
        FROM '{}'
        WITH CSV
    """.format(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/paid_montreal.csv')))
    db.query('ALTER TABLE montreal_paid_temp ALTER COLUMN signposts TYPE integer[] USING signposts::integer[]')
    db.query(mrl.overlay_paid_rules)


def cleanup_table():
    """
    Remove temporary tables
    """
    Logger.info("Cleanup schema")
    db.query("DROP TABLE bad_intersection")
    db.query("DROP TABLE way_intersection")
    db.query("DROP TABLE roads")
    db.query("DROP TABLE signpost_onroad")
    db.query("DROP TABLE slots_likely")
    db.query("DROP TABLE nextpoints")
    db.query("DROP TABLE montreal_paid_temp")
    db.query("DROP TABLE quebec_signpost_temp")
    db.query("DROP TABLE quebec_nextpoints")
    db.query("DROP TABLE quebec_slots_likely")
    db.query("DROP TABLE quebec_paid_slots_raw")
    db.query("DROP TABLE quebec_bornes_raw")
    db.query("DROP TABLE quebec_bornes_clustered")
    db.query("DROP TABLE permit_zones")
    db.query("DROP TABLE slots_temp")


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


def run():
    """
    Run the entire pipeline
    """
    Logger.debug("Loading extension fuzzystrmatch")
    db.query("create extension if not exists fuzzystrmatch")
    db.query("create extension if not exists intarray")

    Logger.info("Loading custom functions")
    db.query(plfunctions.st_isleft_func)
    db.query(plfunctions.array_sort)
    db.query(plfunctions.get_max_range)

    process_osm()

    # create common tables
    db.query(common.create_rules)
    db.create_index('rules', 'code')
    db.query(common.create_slots_temp)
    db.query(common.create_slots)
    db.query(common.create_corrections)
    db.query(common.create_parking_lots_raw)
    db.query(common.create_parking_lots)

    Logger.info("Processing parking lot / garage data")
    insert_raw_lots("lots_stationnement_mtl.csv")
    insert_parking_lots()

    process_montreal()
    process_quebec()

    Logger.info("Shorten slots that intersect with roads or other slots")
    db.query(common.cut_slots_crossing_roads.format(offset=LINE_OFFSET))
    db.query(common.cut_slots_crossing_slots)

    Logger.info("Aggregating like slots")
    db.create_index('slots', 'id')
    db.create_index('slots', 'geom', index_type='gist')
    db.create_index('slots', 'rules', index_type='gin')
    db.query(common.aggregate_like_slots)
    db.query(common.create_client_data)
    db.vacuum_analyze('public', 'slots')

    Logger.info("Mapping corrections to new slots")
    db.query(common.process_corrected_rules)
    db.query(common.process_corrections)

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


def insert_raw_lots(filename):
    db.query("""
        COPY parking_lots_raw (name, operator, address, description, lun_normal, mar_normal, mer_normal,
            jeu_normal, ven_normal, sam_normal, dim_normal, hourly_normal, daily_normal, lun_special,
            mar_special, mer_special, jeu_special, ven_special, sam_special, dim_special, hourly_special,
            daily_special, lun_free, mar_free, mer_free, jeu_free, ven_free, sam_free, dim_free,
            indoor, handicap, clerk, valet, lat, long, active)
        FROM '{}'
        WITH CSV HEADER
    """.format(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', filename)))


def insert_parking_lots():
    columns = ["name", "operator", "address", "description", "agenda", "attrs", "geom", "active", "geojson"]
    days = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]
    lots, queries = [], []
    for row in db.query("""
        SELECT *, ST_Transform(ST_SetSRID(ST_MakePoint(long, lat), 4326), 3857) AS geom
        FROM parking_lots_raw
    """, namedtuple=True):
        lot = [(x.decode('utf-8') if x else '') for x in [row.name, row.operator, row.address, row.description]]
        agenda = {}
        for x in range(1,8):
            agenda[str(x)] = []
            if getattr(row, days[x - 1] + "_normal"):
                y = getattr(row, days[x - 1] + "_normal")
                agenda[str(x)].append({"hours": [float(z) for z in y.split(",")],
                    "hourly": row.hourly_normal or None, "daily": row.daily_normal or None})
            if getattr(row, days[x - 1] + "_special"):
                y = getattr(row, days[x - 1] + "_special")
                agenda[str(x)].append({"hours": [float(z) for z in y.split(",")],
                    "hourly": row.hourly_special or None, "daily": row.daily_special or None})
            if getattr(row, days[x - 1] + "_free"):
                y = getattr(row, days[x - 1] + "_free")
                agenda[str(x)].append({"hours": [float(z) for z in y.split(",")], "hourly": 0})
        lot.append(json.dumps(agenda))
        lot.append(json.dumps({"indoor": row.indoor, "handicap": row.handicap,
            "clerk": row.clerk, "valet": row.valet}))
        lot.append(row.geom)
        lot.append(row.active)
        lots.append(lot)
    for x in lots:
        queries.append("""
            INSERT INTO parking_lots ({}) VALUES ('{}', '{}', '{}', '{}', '{}'::jsonb, '{}'::jsonb,
                '{}'::geometry, '{}', ST_AsGeoJSON(ST_Transform('{geom}'::geometry, 4326))::jsonb)
        """.format(",".join(columns), *[y for y in x], geom=x[-2]))
    db.queries(queries)
