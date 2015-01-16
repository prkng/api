# -*- coding: utf-8 -*-
from __future__ import unicode_literals


create_sign = """
DROP TABLE IF EXISTS sign;
CREATE TABLE sign (
    id integer PRIMARY KEY
    , geom geometry(Point, 3857)
    , direction smallint -- direction the rule applies (0: both side, 1: north, 2: south)
    , signpost integer NOT NULL
    , elevation smallint -- higher is prioritary
    , code varchar -- code of rule
)
"""


insert_sign = """
INSERT INTO sign
(
    id
    , geom
    , direction
    , signpost
    , elevation
    , code
)
SELECT
    p.panneau_id_pan
    , pt.geom
    , case p.fleche_pan
        when 2 then 1
        when 3 then 2
        when 0 then 0
        else NULL
      end as direction
    , pt.poteau_id_pot
    , p.position_pop
    , p.code_rpa
FROM montreal_descr_panneau p
JOIN montreal_poteaux pt on pt.poteau_id_pot = p.poteau_id_pot
WHERE
    pt.description_rep = 'Réel'
"""


create_signpost = """
DROP TABLE IF EXISTS signpost;
CREATE TABLE signpost (
    id integer PRIMARY KEY
    , geobase_id integer
    , geom geometry(Point, 3857)
)
"""

# insert signpost that have valid signs on it
insert_signpost = """
INSERT INTO signpost
    SELECT
        distinct s.signpost
        , pt.trc_id::integer
        , pt.geom
    FROM sign s
    JOIN montreal_poteaux pt ON pt.poteau_id_pot = s.signpost
"""


# try to match osm ways with geobase
match_roads_geobase = """
DROP TABLE IF EXISTS roads_geobase;
CREATE TABLE roads_geobase (
    id integer
    , osm_id bigint
    , name varchar
    , geobase_name varchar
    , id_trc integer
    , geom geometry(linestring, 3857)
);

WITH tmp as (
SELECT
    o.*
    , m.nom_voie as geobase_name
    , m.id_trc
    , rank() over (
        partition by o.id order by
          ST_HausdorffDistance(o.geom, m.geom)
          , levenshtein(o.name, m.nom_voie)
          , abs(st_length(o.geom) - st_length(m.geom)) / greatest(st_length(o.geom), st_length(m.geom))
      ) as rank
FROM roads o
JOIN montreal_geobase m on o.geom && st_expand(m.geom, 10)
WHERE st_contains(st_buffer(m.geom, 30), o.geom)
)
INSERT INTO roads_geobase
SELECT
    id
    , osm_id
    , name
    , geobase_name
    , id_trc
    , geom
FROM tmp
WHERE rank = 1;

-- invert buffer comparison to catch more ways
WITH tmp as (
SELECT
    o.*
    , m.nom_voie as geobase_name
    , m.id_trc
    , rank() over (
        partition by o.id order by
            ST_HausdorffDistance(o.geom, m.geom)
            , levenshtein(o.name, m.nom_voie)
            , abs(st_length(o.geom) - st_length(m.geom)) / greatest(st_length(o.geom), st_length(m.geom))
      ) as rank
FROM roads o
LEFT JOIN roads_geobase orig on orig.id = o.id
JOIN montreal_geobase m on o.geom && st_expand(m.geom, 10)
WHERE st_contains(st_buffer(o.geom, 30), m.geom)
      AND orig.id is NULL
)
INSERT INTO roads_geobase
SELECT
    id
    , osm_id
    , name
    , geobase_name
    , id_trc
    , geom
FROM tmp
WHERE rank = 1

"""

project_signposts = """
DROP TABLE IF EXISTS signpost_onroad;
CREATE TABLE signpost_onroad AS
    SELECT
        distinct on (sp.id) sp.id  -- hack to prevent duplicata, FIXME
        , s.id as road_id
        , st_closestpoint(s.geom, sp.geom)::geometry(point, 3857) as geom
        , st_isleft(s.geom, sp.geom) as isleft
    FROM signpost sp
    JOIN roads_geobase s on s.id_trc = sp.geobase_id;

SELECT id from signpost_onroad group by id having count(*) > 1
"""

create_slots_likely = """
DROP TABLE IF EXISTS slots_likely;
CREATE TABLE slots_likely(
    id serial
    , signposts integer[]
    , geom geometry(linestring, 3857)
);
"""

insert_slots_likely = """
WITH selected_roads AS (
    SELECT
        r.id as rid
        , r.geom as rgeom
        , p.id as pid
        , p.geom as pgeom
    FROM roads_geobase r, signpost_onroad p
    where r.geom && p.geom
        AND r.id = p.road_id
        AND p.isleft = {isleft}
), point_list AS (
    SELECT
        distinct rid
        , 0 as position
        , 0 as signpost
    FROM selected_roads
UNION ALL
    SELECT
        distinct rid
        , 1 as position
        , 0 as signpost
    FROM selected_roads
UNION ALL
    SELECT
        rid
        , st_line_locate_point(rgeom, pgeom) as position
        , pid as signpost
    FROM selected_roads
), loc_with_idx as (
    SELECT
        rid
        , position
        , rank() over (partition by rid order by position) as idx
        , signpost
    FROM point_list
)
INSERT INTO slots_likely (signposts, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (rid)
JOIN roads_geobase w on w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""

# one slot per sign
create_slots = """
DROP TABLE IF EXISTS slots;
CREATE TABLE slots (
    id integer
    , code varchar
    , description varchar
    , season_start varchar
    , season_end varchar
    , time_max_parking float
    , time_start float
    , time_end float
    , time_duration float
    , days int[]
    , special_days varchar
    , restrict_typ varchar
    , direction smallint
    , signpost integer NOT NULL
    , elevation smallint
    , geom geometry(linestring, 3857)
    , geojson jsonb
)
"""

insert_slots_bothsides = """
WITH tmp as (
SELECT
    s.id
    , s.direction
    , s.signpost
    , s.elevation
    , s.code
    , CASE
        WHEN min(spo.isleft) = 1 then
            ST_OffsetCurve(
                st_linemerge(st_union(sl.geom))
                , 8
                , 'quad_segs=4 join=round'
            )
        ELSE
            ST_OffsetCurve(
                st_linemerge(st_union(sl.geom))
                , -8
                , 'quad_segs=4 join=round'
            )
      END as geom
FROM sign s
JOIN signpost_onroad spo on s.signpost = spo.id
JOIN slots_likely sl on ARRAY[s.signpost] <@ sl.signposts
group by s.id
having direction = 0
)
INSERT INTO slots(
    id
    , code
    , description
    , season_start
    , season_end
    , time_max_parking
    , time_start
    , time_end
    , time_duration
    , days
    , special_days
    , restrict_typ
    , direction
    , signpost
    , elevation
    , geom
    , geojson
)
SELECT
    t.id
    , t.code
    , p.description
    , p.season_start
    , p.season_end
    , p.time_max_parking
    , p.time_start
    , p.time_end
    , p.time_duration
    , CASE p.daily
        WHEN 1 THEN ARRAY[1, 2, 3, 4, 5, 6, 7]
        ELSE
            array_remove(
                ARRAY[lun*1] ||
                ARRAY[mar*2] ||
                ARRAY[mer*3] ||
                ARRAY[jeu*4] ||
                ARRAY[ven*5] ||
                ARRAY[sam*6] ||
                ARRAY[dim*7]
                ,NULL
            )
      END as days
    , p.special_days
    , p.restrict_typ
    , t.direction
    , t.signpost
    , t.elevation
    , t.geom
    , ST_AsGeoJSON(st_transform(t.geom, 4326))::jsonb
FROM tmp t
JOIN montreal_rules_translation p on p.code = t.code

"""

insert_slots_north_south = """
WITH tmp as (
SELECT
    s.id
    , s.code
    , s.direction
    , s.signpost
    , s.elevation
    , sl.geom
    , case
        when st_equals(st_startpoint(sl.geom), spo.geom) then st_pointN(sl.geom, 2)
        when st_equals(st_endpoint(sl.geom), spo.geom) then st_pointN(st_reverse(sl.geom), 2)
        else NULL
      end as nextpoint
    , spo.isleft
FROM sign s
JOIN signpost_onroad spo on spo.id = s.signpost
JOIN slots_likely sl on ARRAY[s.signpost] <@ sl.signposts
where direction = {direction}
), ranked as (
SELECT
    *,
    rank() over (partition by id order by st_y(nextpoint) {y_ordering}) as rank
FROM tmp
), raw as (
SELECT
    id
    , code
    , direction
    , signpost
    , elevation
    , CASE
        WHEN isleft = 1 then
            ST_OffsetCurve(geom, 8, 'quad_segs=4 join=round')
        ELSE
            ST_OffsetCurve(geom, -8, 'quad_segs=4 join=round')
      END as geom
FROM ranked
WHERE rank = 1
) INSERT INTO slots(
    id
    , code
    , description
    , season_start
    , season_end
    , time_max_parking
    , time_start
    , time_end
    , time_duration
    , days
    , special_days
    , restrict_typ
    , direction
    , signpost
    , elevation
    , geom
    , geojson
)
SELECT
    r.id
    , r.code
    , p.description
    , p.season_start
    , p.season_end
    , p.time_max_parking
    , p.time_start
    , p.time_end
    , p.time_duration
    , CASE p.daily
        WHEN 1 THEN ARRAY[1, 2, 3, 4, 5, 6, 7]
        ELSE
            array_remove(
                ARRAY[lun*1] ||
                ARRAY[mar*2] ||
                ARRAY[mer*3] ||
                ARRAY[jeu*4] ||
                ARRAY[ven*5] ||
                ARRAY[sam*6] ||
                ARRAY[dim*7]
                ,NULL
            )
      END as days
    , p.special_days
    , p.restrict_typ
    , r.direction
    , r.signpost
    , r.elevation
    , r.geom
    , ST_AsGeoJSON(st_transform(r.geom, 4326))::jsonb
FROM raw r
JOIN montreal_rules_translation p on p.code = r.code
"""


remove_empty_days = """
DELETE FROM slots
WHERE cardinality(days) = 0
RETURNING id
"""
