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
    'restrict_typ',
    'permit_no'
)

create_rules = """
DROP TABLE IF EXISTS rules;
CREATE TABLE rules (
    id serial PRIMARY KEY
    , code varchar
    , description varchar
    , season_start varchar DEFAULT ''
    , season_end varchar DEFAULT ''
    , time_max_parking float DEFAULT 0.0
    , agenda jsonb
    , special_days varchar DEFAULT ''
    , restrict_typ varchar DEFAULT ''
    , permit_no varchar
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
    , permit_no
FROM {source}
"""

create_corrections = """
CREATE TABLE IF NOT EXISTS corrections
(
  id serial PRIMARY KEY,
  created timestamp DEFAULT NOW(),
  initials varchar,
  city varchar,
  signposts integer[],
  code varchar,
  description varchar,
  season_start varchar,
  season_end varchar,
  time_max_parking float,
  agenda jsonb,
  special_days varchar,
  restrict_typ varchar
)
"""

process_corrected_rules = """
WITH s AS (
  -- get the rule if it already exists
  SELECT c.id, r.code, r.description FROM corrections c
    LEFT JOIN rules r
       ON r.season_start     = c.season_start
      AND r.season_end       = c.season_end
      AND r.time_max_parking = c.time_max_parking
      AND r.agenda           = c.agenda
      AND r.special_days     = c.special_days
      AND r.restrict_typ     = c.restrict_typ
), i AS (
  -- if it doesn't exist, create it
  INSERT INTO rules
    (code, description, season_start, season_end, time_max_parking,
      agenda, special_days, restrict_typ)
    SELECT c.code, c.description, c.season_start, c.season_end, c.time_max_parking,
      c.agenda, c.special_days, c.restrict_typ
      FROM corrections c, s
      WHERE c.id = s.id AND s.code IS NULL
      RETURNING code, description
)
-- finally update the original correction w/ proper code/desc if needed
UPDATE corrections c
  SET code = s.code, description = s.description
  FROM s
  WHERE c.id = s.id
    AND c.code <> s.code
"""

process_corrections = """
WITH r AS (
  SELECT
    signposts,
    array_to_json(
      array_agg(distinct
      json_build_object(
        'code', code,
        'description', description,
        'season_start', season_start,
        'season_end', season_end,
        'agenda', agenda,
        'time_max_parking', time_max_parking,
        'special_days', special_days,
        'restrict_typ', restrict_typ
      )::jsonb
    ))::jsonb AS rules
  FROM corrections
  GROUP BY signposts
)
UPDATE slots s
  SET rules = r.rules
  FROM r
  WHERE s.signposts = r.signposts
    AND s.rules <> r.rules
"""

create_slots_temp = """
DROP TABLE IF EXISTS slots_temp;
CREATE TABLE slots_temp
(
  id serial PRIMARY KEY,
  rid integer,
  position float,
  signposts integer[],
  rules jsonb,
  way_name varchar,
  geom geometry(LineString,3857),
  geojson jsonb,
  button_location jsonb
)
"""

create_slots = """
DROP TABLE IF EXISTS slots;
CREATE TABLE slots
(
  id serial PRIMARY KEY,
  rid integer,
  signposts integer[],
  rules jsonb,
  way_name varchar,
  geom geometry(LineString,3857),
  geojson jsonb,
  button_location jsonb
)
"""

aggregate_like_slots = """
DO
$$
DECLARE
  slot record;
  id_match integer;
BEGIN
  FOR slot IN SELECT * FROM slots_temp ORDER BY rid, position LOOP
    SELECT id FROM slots s
      WHERE slot.rid = s.rid
        AND ST_Touches(slot.geom, s.geom)
        AND slot.rules = s.rules
      LIMIT 1 INTO id_match;

    IF id_match IS NULL THEN
      INSERT INTO slots (rid, signposts, rules, geom, way_name) VALUES
        (slot.rid, ARRAY[slot.signposts], slot.rules, slot.geom, slot.way_name);
    ELSE
      UPDATE slots SET geom =
        (CASE WHEN ST_Intersection(slot.geom, geom) = ST_EndPoint(geom)
            THEN ST_MakeLine(geom, slot.geom)
            ELSE ST_MakeLine(slot.geom, geom)
        END),
        signposts = (signposts || ARRAY[slot.signposts])
      WHERE slots.id = id_match;
    END IF;
  END LOOP;
END;
$$ language plpgsql;
"""

cut_slots_crossing_slots = """
UPDATE slots_temp s set geom = (
with tmp as (
select
    array_sort(
        array_agg(
            ST_Line_Locate_Point(s.geom, st_intersection(s.geom, o.geom))
        )
    ) as locations
from slots_temp o
where st_crosses(s.geom, o.geom) and s.id != o.id
and st_geometrytype(st_intersection(s.geom, o.geom)) = 'ST_Point'
)
select
    st_linesubstring(s.geom, locs.start, locs.stop)::geometry('linestring', 3857)
from tmp, get_max_range(tmp.locations) as locs
)
where exists (
    select 1 from slots_temp a
    where st_crosses(s.geom, a.geom)
          and s.id != a.id
          and st_geometrytype(st_intersection(s.geom, a.geom)) = 'ST_Point'
)
"""

cut_slots_crossing_roads = """
UPDATE slots_temp s set geom = (
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

create_client_data = """
UPDATE slots SET
    geojson = ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
    button_location = json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)),
        'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)))::jsonb
"""
