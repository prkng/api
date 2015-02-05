# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from collections import namedtuple, defaultdict
from itertools import groupby
import json

from prkng.logger import Logger
from prkng import create_app
from prkng.database import PostgresWrapper

import osm
import montreal as mrl
import quebec as qbc
import plfunctions
import common


CONFIG = create_app().config
db = PostgresWrapper(
    "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
    "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))


def process_montreal():
    """
    process montreal data and generate parking slots
    """
    Logger.info("Processing Montreal DATA")

    Logger.debug('Loadind and translating montreal rules')
    insert_rules('montreal_rules_translation')
    db.vacuum_analyze('public', 'rules')

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

    Logger.info("Projecting signposts on road")
    duplicates = db.query(mrl.project_signposts)
    if duplicates:
        Logger.warning("Duplicates found for projected signposts : {}"
                       .format(str(duplicates)))
    db.create_index('signpost_onroad', 'id')
    db.create_index('signpost_onroad', 'road_id')
    db.create_index('signpost_onroad', 'isleft')
    db.create_index('signpost_onroad', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'signpost_onroad')

    Logger.info("Creating slots between signposts")
    db.query(mrl.create_slots_likely)
    db.query(mrl.insert_slots_likely.format(isleft=1, offset=10))
    db.query(mrl.insert_slots_likely.format(isleft=-1, offset=-10))
    db.create_index('slots_likely', 'id')
    db.create_index('slots_likely', 'signposts', index_type='gin')
    db.create_index('slots_likely', 'geom', index_type='gist')
    db.vacuum_analyze('public', 'slots_likely')

    db.query(mrl.create_nextpoints_for_signposts)
    db.create_index('nextpoints', 'id')
    db.create_index('nextpoints', 'slot_id')
    db.create_index('nextpoints', 'direction')
    db.vacuum_analyze('public', 'nextpoints')

    db.query(mrl.create_slots)
    db.create_index('slots', 'id')
    db.create_index('slots', 'geom', index_type='gist')
    db.create_index('slots', 'rules', index_type='gin')


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


def run():
    """
    Run the entire pipeline
    """
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

    Logger.info("Loading custom functions")
    db.query(plfunctions.st_isleft_func)
    db.query(plfunctions.date_equality_func)

    # create rule table
    db.query(common.create_rules)
    db.create_index('rules', 'code')

    process_montreal()

    if not CONFIG['DEBUG']:
        cleanup_table()


def insert_rules(from_table):
    """
    Get rules from specific location (montreal, quebec),
    group them, make a simpler model and load them into database
    """
    Logger.debug("Get rules and transform them to a more simple model")
    rules = db.query(
        common.get_rules_from_source.format(source=from_table),
        namedtuple=True
    )
    rules_grouped = group_rules(rules)

    Logger.debug("Load rules into montreal_rule table")

    db.copy_from('public', 'rules', common.rules_columns, [
        [
            json.dumps(val).replace('\\', '\\\\') if isinstance(val, dict) else val
            for val in rule._asdict().values()]
        for rule in rules_grouped
    ])


def group_rules(rules):
    """
    group rules having the same code and contructs an array of
    parking time for each day
    """
    singles = namedtuple('singles', (
        'id', 'code', 'description', 'season_start', 'season_end',
        'time_max_parking', 'agenda', 'special_days', 'restrict_typ'
    ))

    results = []
    days = ('lun', 'mar', 'mer', 'jeu', 'ven', 'sam', 'dim')

    for code, group in groupby(rules, lambda x: x.code):

        day_dict = defaultdict(list)

        for part in group:
            for numday, day in enumerate(days, start=1):
                isok = getattr(part, day) or part.daily
                if not isok:
                    continue
                # others cases
                if part.time_end:
                    day_dict[numday].append([part.time_start, part.time_end])

                elif part.time_duration:
                    fdl, ndays, ldf = split_time_range(part.time_start, part.time_duration)
                    # first day
                    day_dict[numday].append([part.time_start, part.time_start + fdl])

                    for inter_day in xrange(1, ndays + 1):
                        day_dict[numday + inter_day].append([0, 24])
                    # last day
                    if ldf != 0:
                        day_dict[numday].append([0, ldf])

                else:
                    day_dict[numday].append([0, 24])

        # add empty days
        for numday, day in enumerate(days, start=1):
            if not day_dict[numday]:
                day_dict[numday].append(None)

        results.append(singles(
            part.id,
            part.code,
            part.description,
            part.season_start,
            part.season_end,
            part.time_max_parking,
            dict(day_dict),
            part.special_days,
            part.restrict_typ
        ))

    return results


def split_time_range(start_time, duration):
    """
    Given a start time and a duration, returns a 3-tuple containing
    the time left for the current day, a number of plain day left, a number of hours left
    for the last day
    """
    if start_time + duration <= 24:
        # end is inside the first day
        return duration, 0, 0

    time_left_first = 24 - start_time
    plain_days = (duration - time_left_first) // 24
    time_left_last = (duration - time_left_first) % 24
    return time_left_first, int(plain_days), time_left_last
