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
  geom geometry(LineString,3857)
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
  button_location jsonb,
  button_locations jsonb
)
"""

create_parking_lots_raw = """
DROP TABLE IF EXISTS parking_lots_raw;
CREATE TABLE parking_lots_raw (
  id serial primary key,
  name varchar,
  operator varchar,
  address varchar,
  description varchar,
  lun_normal varchar,
  mar_normal varchar,
  mer_normal varchar,
  jeu_normal varchar,
  ven_normal varchar,
  sam_normal varchar,
  dim_normal varchar,
  hourly_normal float,
  daily_normal float,
  lun_special varchar,
  mar_special varchar,
  mer_special varchar,
  jeu_special varchar,
  ven_special varchar,
  sam_special varchar,
  dim_special varchar,
  hourly_special float,
  daily_special float,
  lun_free varchar,
  mar_free varchar,
  mer_free varchar,
  jeu_free varchar,
  ven_free varchar,
  sam_free varchar,
  dim_free varchar,
  indoor boolean,
  handicap boolean,
  clerk boolean,
  valet boolean,
  lat float,
  long float,
  active boolean
)
"""

create_parking_lots = """
DROP TABLE IF EXISTS parking_lots;
CREATE TABLE parking_lots
(
  id serial PRIMARY KEY,
  active boolean,
  name varchar,
  operator varchar,
  address varchar,
  description varchar,
  agenda jsonb,
  attrs jsonb,
  geom geometry(Point,3857),
  geojson jsonb
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
        AND slot.rules = s.rules
        AND ST_DWithin(slot.geom, s.geom, 0.1)
      LIMIT 1 INTO id_match;

    IF id_match IS NULL THEN
      INSERT INTO slots (rid, signposts, rules, geom, way_name) VALUES
        (slot.rid, ARRAY[slot.signposts], slot.rules, slot.geom, slot.way_name);
    ELSE
      UPDATE slots SET geom =
        (CASE WHEN ST_DWithin(ST_StartPoint(slot.geom), ST_EndPoint(geom), 0.5)
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
WITH exclusions AS (
    SELECT s.id, ST_Difference(s.geom, ST_Union(ST_Buffer(r.geom, {offset}, 'endcap=flat join=round'))) AS new_geom
    FROM slots_temp s
    JOIN roads r ON ST_DWithin(s.geom, r.geom, 4)
    GROUP BY s.id, s.geom
), update_original AS (
    DELETE FROM slots_temp
    USING exclusions
    WHERE slots_temp.id = exclusions.id
    RETURNING slots_temp.*
), new_slots AS (
    SELECT
        uo.*,
        CASE ST_GeometryType(ex.new_geom)
            WHEN 'ST_LineString' THEN
                ex.new_geom
            ELSE
                (ST_Dump(ex.new_geom)).geom
        END AS new_geom
    FROM exclusions ex
    JOIN update_original uo ON ex.id = uo.id
)
INSERT INTO slots_temp (rid, position, signposts, rules, way_name, geom)
    SELECT
        rid,
        position,
        signposts,
        rules,
        way_name,
        new_geom
    FROM new_slots
    WHERE ST_Length(new_geom) >= 4
"""

create_client_data = """
UPDATE slots SET
    geojson = ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
    button_location = json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)),
        'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)))::jsonb,
    button_locations = (case when st_length(geom) >= 300 then array_to_json(array[
        json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.333), 4326)),
            'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.333), 4326))),
        json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.666), 4326)),
            'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.666), 4326)))])::jsonb
        else array_to_json(array[button_location])::jsonb end)
"""
