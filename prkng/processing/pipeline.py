# -*- coding: utf-8 -*-
from prkng.logger import Logger
from prkng import create_app
from prkng.database import PostgresWrapper

import osm
import montreal as mrl
import geofunctions


CONFIG = create_app().config
db = PostgresWrapper(
    "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
    "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))


def process_montreal():
    """
    process montreal data to generate parking slots
    """
    Logger.info("Processing Montreal DATA")

    Logger.info("Creating sign table")
    db.query(mrl.create_sign)

    Logger.info("Loading montreal signs")
    db.query(mrl.insert_sign)
    db.create_index('sign', 'geom', index_type='gist')
    db.create_index('sign', 'direction')
    db.create_index('sign', 'elevation')
    db.create_index('sign', 'signpost')
    db.vacuum_analyze('public', 'sign')

    Logger.info("Creating sign posts")
    db.query(mrl.create_signpost)
    db.query(mrl.insert_signpost)
    db.create_index('signpost', 'geom', index_type='gist')
    db.create_index('signpost', 'geobase_id')
    db.vacuum_analyze('public', 'signpost')

    Logger.info("Matching osm roads with geobase")
    db.query(mrl.match_roads_geobase)
    db.create_index('roads_geobase', 'id')
    db.create_index('roads_geobase', 'id_trc')
    db.create_index('roads_geobase', 'osm_id')
    db.create_index('roads_geobase', 'name')
    db.create_index('roads_geobase', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'roads_geobase')

    Logger.info("Creating slots")
    duplicates = db.query(mrl.project_signposts)
    if duplicates:
        Logger.warning("Duplicates found for projected signposts : {}"
                       .format(str(duplicates)))
    db.create_index('signpost_onroad', 'id')
    db.create_index('signpost_onroad', 'road_id')
    db.create_index('signpost_onroad', 'isleft')
    db.create_index('signpost_onroad', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'signpost_onroad')

    db.query(mrl.create_slots_likely)
    db.query(mrl.insert_slots_likely.format(isleft=1, offset=10))
    db.query(mrl.insert_slots_likely.format(isleft=-1, offset=-10))
    db.create_index('slots_likely', 'id')
    db.create_index('slots_likely', 'signposts', index_type='gin')
    db.create_index('slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'slots_likely')

    db.query(mrl.create_slots)
    db.query(mrl.insert_slots_bothsides)
    # insert north direction
    db.query(mrl.insert_slots_north_south.format(direction=1, y_ordering='DESC'))
    # insert south direction
    db.query(mrl.insert_slots_north_south.format(direction=2, y_ordering='ASC'))

    db.create_index('slots', 'geom', index_type='gist')
    db.create_index('slots', 'id')
    db.create_index('slots', 'days', index_type='gin')
    db.create_index('slots', 'signpost')
    db.create_index('slots', 'elevation')
    db.vacuum_analyze('public', 'slots')

    res = db.query(mrl.remove_empty_days)
    if res:
        Logger.debug("Removed {} slots with empty days".format(len(res)))


def cleanup_table():
    """
    Remove temporary tables
    """
    Logger.info("Cleanup schema")
    db.query("DROP TABLE bad_intersection")
    db.query("DROP TABLE way_intersection")
    db.query("DROP TABLE roads_geobase")
    db.query("DROP TABLE signpost_onroad")
    db.query("DROP TABLE slots_likely")


def run():

    Logger.debug("Loading extension fuzzystrmatch")
    db.query("create extension if not exists fuzzystrmatch")

    Logger.info("Filtering osm ways")
    db.query(osm.create_osm_ways)
    db.create_index('osm_ways', 'geom', index_type='gist')
    db.create_index('osm_ways', 'osm_id')
    db.create_index('osm_ways', 'name')

    Logger.info("Creating way intersections from osm lines")
    db.query(osm.create_way_intersection)
    db.create_index('way_intersection', 'way_id')
    db.create_index('way_intersection', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'way_intersection')
    res = db.query(osm.remove_bad_intersection)
    if res:
        Logger.debug("Removed {} bad intersections".format(len(res)))

    Logger.info("Splitting ways on intersections")
    db.query(osm.split_osm_roads)
    db.create_index('roads', 'id')
    db.create_index('roads', 'osm_id')
    db.create_index('roads', 'name')
    db.create_index('roads', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'roads')

    # Logger.info("Loading custom functions")
    db.query(geofunctions.st_isleft)
    db.query(geofunctions.to_time_func)

    process_montreal()

    if not CONFIG['DEBUG']:
        cleanup_table()
