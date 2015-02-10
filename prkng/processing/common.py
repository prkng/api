# -*- coding: utf-8 -*-
from __future__ import unicode_literals


rules_columns = (
    'code',
    'description',
    'season_start',
    'season_end',
    'time_max_parking',
    'agenda',
    'special_days',
    'restrict_typ'
)

create_rules = """
DROP TABLE IF EXISTS rules;
CREATE TABLE rules (
    id serial PRIMARY KEY
    , code varchar UNIQUE
    , description varchar
    , season_start varchar
    , season_end varchar
    , time_max_parking float
    , agenda jsonb
    , special_days varchar
    , restrict_typ varchar
)
"""

get_rules_from_source = """SELECT
    code
    , description
    , season_start
    , season_end
    , time_max_parking
    , time_start
    , time_end
    , time_duration
    , lun
    , mar
    , mer
    , jeu
    , ven
    , sam
    , dim
    , daily
    , special_days
    , restrict_typ
FROM {source}
"""

create_slots = """
DROP TABLE IF EXISTS slots;
CREATE TABLE slots
(
  id integer,
  signposts integer[],
  rules jsonb,
  geom geometry(LineString,3857),
  geojson jsonb
)
"""
