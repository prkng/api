# -*- coding: utf-8 -*-
from __future__ import unicode_literals


create_osm_ways = """
DROP TABLE IF EXISTS osm_ways;
CREATE TABLE osm_ways
AS
SELECT
    osm_id
    , name
    , way as geom
FROM planet_osm_line
WHERE
    name is not NULL
    AND railway is NULL
    AND waterway is NULL
    AND boundary is NULL
    AND leisure is NULL -- avoid parks
    AND landuse is NULL -- avoid industrial areas
    AND st_issimple(way)
UNION ALL
SELECT
    osm_id
    , name
    , (st_dump(st_node(way))).geom as geom
FROM planet_osm_line
WHERE
    name is not NULL
    AND railway is NULL
    AND waterway is NULL
    AND leisure is NULL -- avoid parks
    AND landuse is NULL -- avoid industrial areas
    AND boundary is NULL
    AND NOT st_issimple(way)
"""


create_way_intersection = """
DROP TABLE IF EXISTS way_intersection;
CREATE TABLE way_intersection(
    id serial
    , way_id bigint
    , geom geometry(point, 3857)
);

WITH tmp as (
    SELECT
        a.osm_id as way1
        , b.osm_id as way2
        , (st_dump(st_intersection(a.geom, b.geom))).geom as geom
    FROM osm_ways a
    JOIN osm_ways b
        ON a.geom && b.geom
        AND a.osm_id != b.osm_id
)
INSERT INTO way_intersection (way_id, geom)
    SELECT way1, geom
    FROM tmp WHERE st_geometrytype(geom) = 'ST_Point'
UNION -- remove duplicates
    SELECT way2, geom
    FROM tmp WHERE st_geometrytype(geom) = 'ST_Point';
"""

remove_bad_intersection = """
DROP TABLE IF EXISTS bad_intersection;
CREATE TABLE bad_intersection(
    id integer PRIMARY KEY
);

with tmp as (
    SELECT
        wi.id
        , ST_Relate(wi.geom, pl.geom, '*0*******') as touch
    FROM way_intersection wi
    JOIN osm_ways pl on wi.geom && pl.geom AND st_intersects(wi.geom, pl.geom)
), unn as (
    SELECT
        id
        , array_to_string(array_agg(touch), '') as rel
    FROM tmp
    GROUP BY id having count(*) = 2
)
INSERT INTO bad_intersection
SELECT DISTINCT id
FROM unn where rel = 'tt';

DELETE FROM way_intersection where id in (SELECT * from bad_intersection);
"""

# split lines to create segments between intersections
split_osm_roads = """
DROP TABLE IF EXISTS roads;
CREATE TABLE roads(
    id serial
    , osm_id bigint
    , name varchar
    , geom geometry(linestring, 3857)
);

WITH tmp AS (
    SELECT
        osm_id
        , 0 as position
    FROM osm_ways
UNION ALL
    SELECT
        osm_id
        , 1 as position
    FROM osm_ways
UNION ALL
    SELECT
        l.osm_id
        , st_line_locate_point(l.geom, p.geom) as position
    FROM osm_ways l, way_intersection p
    where st_intersects(l.geom, p.geom)
        AND l.osm_id = p.way_id
        AND not ST_Relate(p.geom, l.geom, '*0*******') -- exclude endpoints
), loc_with_idx as (
    SELECT
        osm_id
        , position
        , dense_rank() over (partition by osm_id order by position) as idx
    FROM tmp
)
INSERT INTO roads (osm_id, name, geom)
SELECT
    distinct  -- avoid rings without intersections
    w.osm_id
    , w.name
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (osm_id)
JOIN osm_ways w using (osm_id)
WHERE loc2.idx = loc1.idx+1;

"""
