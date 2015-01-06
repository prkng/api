# -*- coding: utf-8 -*-
from prkng.logger import Logger
from prkng import create_app
from prkng.database import PostgresWrapper

from .sql import *

CONFIG = create_app().config
db = PostgresWrapper(
    "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
    "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))


def process_montreal():
    """
    process montreal data to generate parking slots
    """
    Logger.info("Load montreal datas into sign table")
    db.query(load_montreal)
    db.create_index('sign', 'geom', index_type='gist')
    db.create_index('sign', 'direction')
    db.vacuum_analyze('public', 'sign')

    Logger.info("Find the nearest way for each sign")
    db.query(sign_way)
    db.create_index('sign_way', 'osm_id')
    db.vacuum_analyze('public', 'sign_way')

    Logger.info("Project each sign on closest way and create a point")
    db.query(project_sign)
    db.create_index('sign_projected', 'id')
    db.create_index('sign_projected', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'sign_projected')

    Logger.info("Generate slots segments")
    db.query(generate_slots_segments)
    db.create_index('slots_double', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'slots_double')

    db.query(split_final_slots)
    db.create_index('slots', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'slots')

    # remove tmp table
    # actually keep some of them for debug #FIXME
    db.query("DROP TABLE slots_double; DROP TABLE sign_way")


def main():
    Logger.info("Create sign table")
    db.query(create_sign)

    Logger.info("Creates way intersections from osm lines")
    db.query(create_way_intersection)
    db.create_index('way_intersection', 'way_id')
    db.create_index('way_intersection', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'way_intersection')

    process_montreal()
