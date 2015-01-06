# -*- coding: utf-8 -*-
from __future__ import unicode_literals

create_sign = """
DROP TABLE IF EXISTS sign;
CREATE TABLE sign (
    id serial
    , code varchar
    , description varchar
    , season_start varchar
    , season_end varchar
    , time_max_parking float
    , time_start float
    , time_end float
    , time_duration float
    , lun smallint
    , mar smallint
    , mer smallint
    , jeu smallint
    , ven smallint
    , sam smallint
    , dim smallint
    , daily float
    , special_days varchar
    , restrict_typ varchar
    , geom geometry(Point, 3857)
    , direction smallint -- direction the rule applies (0: both side, 1: north, 2: south)
)
"""

load_montreal = """
INSERT INTO sign
(
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
    , geom
    , direction
)

SELECT
    r.code
    , r.description
    , r.season_start
    , r.season_end
    , r.time_max_parking
    , r.time_start
    , r.time_end
    , r.time_duration
    , r.lun
    , r.mar
    , r.mer
    , r.jeu
    , r.ven
    , r.sam
    , r.dim
    , r.daily
    , r.special_days
    , r.restrict_typ
    , p.geom
    , case p.fleche_pan
          when 2 then 1
          when 3 then 2
          when 0 then 0
      else NULL
    end as direction
FROM montreal_panneau p
JOIN montreal_rules_translation r ON p.code_rpa = r.code
WHERE r.code in ('SD-TT', 'SD-TC'); -- only basic rules for tests
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
        , (st_dump(st_intersection(a.way, b.way))).geom as geom
    FROM planet_osm_line a
    JOIN planet_osm_line b ON a.way && b.way AND a.osm_id != b.osm_id
    WHERE a.name IS NOT NULL AND b.name IS NOT NULL
)
INSERT INTO way_intersection (way_id, geom)
    SELECT way1, geom
    FROM tmp WHERE st_geometrytype(geom) = 'ST_Point'
UNION
    SELECT way2, geom
    FROM tmp WHERE st_geometrytype(geom) = 'ST_Point'
"""

# associate each sign with the closest osm way
sign_way = """
DROP TABLE IF EXISTS sign_way;
CREATE TABLE sign_way(
    id integer PRIMARY KEY
    ,osm_id bigint
    ,dist float
);

WITH tmp AS (
select
    rank() over (partition by s.id order by st_distance(l.way, s.geom)) as rank
    , s.id
    , l.osm_id
    , st_distance(l.way, s.geom) as dist
FROM sign s
JOIN planet_osm_line l on l.way && s.geom
    WHERE l.name is not null
)
INSERT INTO sign_way (id, osm_id, dist)
    SELECT id, osm_id, dist
    FROM tmp
        WHERE rank = 1
        AND dist < 30
"""

project_sign = """
DROP TABLE IF EXISTS sign_projected;
CREATE TABLE sign_projected
AS
    SELECT
        s.id
        , sw.osm_id
        , st_closestpoint(l.way, s.geom)::geometry(point, 3857) as geom
    FROM sign_way sw
    JOIN sign s on sw.id = s.id
    JOIN planet_osm_line l on sw.osm_id = l.osm_id
"""

generate_slots_segments = """
DROP TABLE IF EXISTS slots_double;
CREATE TABLE slots_double (
    id integer -- sign id
    ,osm_id bigint
    ,geom geometry(linestring, 3857)
);

-- north
WITH selection AS (
select
    sp.id,
    l.osm_id,
    ST_LineLocatePoint(l.way, sp.geom) as startpoint,
    ST_LineLocatePoint(l.way, (
                select geom from way_intersection where
                way_id = l.osm_id
                and st_y(geom) > st_y(sp.geom)
                order by st_distance(geom, sp.geom)
                limit 1
               )) as endpoint,
        s.geom as sgeom,
        sp.geom as spgeom,
        l.way as lway
from sign_projected sp
    join sign s on s.id = sp.id
    join planet_osm_line l on l.osm_id = sp.osm_id
    where s.direction = 1
    and exists (select 1 from way_intersection it where it.way_id = sp.osm_id)
) INSERT INTO slots_double (id, osm_id, geom)
SELECT
    id
    , osm_id
    , ST_OffsetCurve(
            ST_LineSubstring(lway,
                least(startpoint, endpoint),
                greatest(startpoint, endpoint)
            ),
            st_distance(sgeom, spgeom),
            'quad_segs=4 join=round'
    ) as geom
    from selection
    where startpoint is not NULL and endpoint is not NULL
    -- last filter = hack to prevent intersections which are under the projected point
    -- returning a NULL value for ST_LineLocatePoint
UNION ALL
SELECT
    id
    , osm_id
    , ST_OffsetCurve(
        ST_LineSubstring(lway,
            least(startpoint, endpoint),
            greatest(startpoint, endpoint)
        ),
        -st_distance(sgeom, spgeom),
        'quad_segs=4 join=round'
    ) as geom
    from selection
    where startpoint is not NULL and endpoint is not NULL
;

-- south
WITH selection AS (
select
    sp.id,
    l.osm_id,
    ST_LineLocatePoint(l.way, sp.geom) as startpoint,
    ST_LineLocatePoint(l.way, (
                select geom from way_intersection where
                way_id = l.osm_id
                and st_y(geom) < st_y(sp.geom)
                order by st_distance(geom, sp.geom)
                limit 1
               )) as endpoint,
        s.geom as sgeom,
        sp.geom as spgeom,
        l.way as lway
from sign_projected sp
    join sign s on s.id = sp.id
    join planet_osm_line l on l.osm_id = sp.osm_id
    where s.direction = 2
    and exists (select 1 from way_intersection it where it.way_id = sp.osm_id)
) INSERT INTO slots_double (id, osm_id, geom)
SELECT
    id
    , osm_id
    , ST_OffsetCurve(
        ST_LineSubstring(lway,
            least(startpoint, endpoint),
            greatest(startpoint, endpoint)
        ),
        st_distance(sgeom, spgeom),
        'quad_segs=4 join=round'
    ) as geom
    from selection
    where startpoint is not NULL and endpoint is not NULL
UNION ALL
SELECT
    id
    , osm_id
    , ST_OffsetCurve(
        ST_LineSubstring(lway,
            least(startpoint, endpoint),
            greatest(startpoint, endpoint)
        ),
        -st_distance(sgeom, spgeom),
        'quad_segs=4 join=round'
    ) as geom
    from selection
    where startpoint is not NULL and endpoint is not NULL
;

-- both sides
"""

split_final_slots = """
DROP TABLE IF EXISTS slots;
CREATE TABLE slots as
SELECT
    sd.id
    , sd.osm_id
    , ST_LineSubstring(sd.geom, 0.05, 0.95) as geom -- better rendering
    , si.code
    , si.description
    , si.season_start
    , si.season_end
    , si.time_max_parking
    , si.time_start
    , si.time_end
    , si.time_duration
    , si.lun
    , si.mar
    , si.mer
    , si.jeu
    , si.ven
    , si.sam
    , si.dim
    , si.daily
    , si.special_days
    , si.restrict_typ
FROM slots_double sd
JOIN sign si ON sd.geom && si.geom
             AND st_intersects(sd.geom, st_buffer(si.geom, 2))
             AND sd.id = si.id
"""
