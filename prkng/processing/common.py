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

get_rules_from_source = """
SELECT
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
  id serial PRIMARY KEY,
  signposts integer[],
  rules jsonb,
  geom geometry(LineString,3857),
  geojson jsonb
)
"""

cut_slots_crossing_slots = """
UPDATE slots s set geom = (
with tmp as (
select
    array_sort(
        array_agg(
            ST_Line_Locate_Point(s.geom, st_intersection(s.geom, o.geom))
        )
    ) as locations
from slots o
where st_crosses(s.geom, o.geom) and s.id != o.id
and st_geometrytype(st_intersection(s.geom, o.geom)) = 'ST_Point'
)
select
    st_linesubstring(s.geom, locs.start, locs.stop)::geometry('linestring', 3857)
from tmp, get_max_range(tmp.locations) as locs
)
where exists (
    select 1 from slots a
    where st_crosses(s.geom, a.geom)
          and s.id != a.id
          and st_geometrytype(st_intersection(s.geom, a.geom)) = 'ST_Point'
)
"""

cut_slots_crossing_roads = """
UPDATE slots s set geom = (
with tmp as (
select
    array_sort(
        array_agg(
            ST_Line_Locate_Point(s.geom, st_intersection(s.geom, o.geom))
        )
    ) as locations
from roads o
where st_crosses(s.geom, o.geom)
and st_geometrytype(st_intersection(s.geom, o.geom)) = 'ST_Point'
)
select
    st_linesubstring(s.geom, locs.start, locs.stop)::geometry('linestring', 3857)
from tmp, get_max_range(tmp.locations) as locs
)
where exists (
    select 1 from roads a
    where st_crosses(s.geom, a.geom)
          and st_geometrytype(st_intersection(s.geom, a.geom)) = 'ST_Point'
)
"""

update_geojson_slots = """
UPDATE slots set geojson = ST_AsGeoJSON(st_transform(geom, 4326))::jsonb
"""
