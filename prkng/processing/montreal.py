# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS montreal_sign;
CREATE TABLE montreal_sign (
    id serial PRIMARY KEY
    , sid integer NOT NULL
    , geom geometry(Point, 3857)
    , direction smallint -- direction the rule applies (0: both side, 1: left, 2: right)
    , signpost integer NOT NULL
    , elevation smallint -- higher is prioritary
    , code varchar -- code of rule
    , description varchar -- description of rule
)
"""

# insert montreal signs with associated postsigns
# only treat fleche_pan 0, 2, 3 for direction
# don't know what others mean
insert_sign = """
INSERT INTO montreal_sign
(
    sid
    , geom
    , direction
    , signpost
    , elevation
    , code
    , description
)
SELECT
    p.panneau_id_pan
    , pt.geom
    , case p.fleche_pan
        when 2 then 1 -- Left
        when 3 then 2 -- Right
        when 0 then 0 -- both sides
        when 8 then 0 -- both sides
        else NULL
      end as direction
    , pt.poteau_id_pot
    , p.position_pop
    , p.code_rpa
    , p.description_rpa
FROM montreal_descr_panneau p
JOIN montreal_poteaux pt on pt.poteau_id_pot = p.poteau_id_pot
JOIN rules r on r.code = p.code_rpa -- only keep those existing in rules
WHERE
    pt.description_rep = 'Réel'
    AND p.description_rpa not ilike '%panonceau%'
    AND p.code_rpa !~ '^R[BCGHK].*' -- don't match rules starting with 'R*'
    AND p.code_rpa <> 'RD-TT' -- don't match 'debarcaderes'
    AND substring(p.description_rpa, '.*\((flexible)\).*') is NULL
    AND p.fleche_pan in (0, 2, 3, 8)
"""

# create signpost table
create_signpost = """
DROP TABLE IF EXISTS montreal_signpost;
CREATE TABLE montreal_signpost (
    id integer PRIMARY KEY
    , geobase_id integer
    , geom geometry(Point, 3857)
)
"""

# insert only signpost that have signs on it
insert_signpost = """
INSERT INTO montreal_signpost
    SELECT
        distinct s.signpost
        , pt.trc_id::integer
        , pt.geom
    FROM montreal_sign s
    JOIN montreal_poteaux pt ON pt.poteau_id_pot = s.signpost
"""


# try to match osm ways with geobase
match_roads_geobase = """
DROP TABLE IF EXISTS montreal_roads_geobase;
CREATE TABLE montreal_roads_geobase (
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
INSERT INTO montreal_roads_geobase
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
LEFT JOIN montreal_roads_geobase orig on orig.id = o.id
JOIN montreal_geobase m on o.geom && st_expand(m.geom, 10)
WHERE st_contains(st_buffer(o.geom, 30), m.geom)
      AND orig.id is NULL
)
INSERT INTO montreal_roads_geobase
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

# project signposts on road and
# determine if they were on the left side or right side of the road
project_signposts = """
DROP TABLE IF EXISTS montreal_signpost_onroad;
CREATE TABLE montreal_signpost_onroad AS
    SELECT
        distinct on (sp.id) sp.id  -- hack to prevent duplicata, FIXME
        , s.id as road_id
        , st_closestpoint(s.geom, sp.geom)::geometry(point, 3857) as geom
        , st_isleft(s.geom, sp.geom) as isleft
    FROM montreal_signpost sp
    JOIN montreal_roads_geobase s on s.id_trc = sp.geobase_id
    ORDER BY sp.id, ST_Distance(s.geom, sp.geom);

SELECT id from montreal_signpost_onroad group by id having count(*) > 1
"""

# how many signposts have been projected ?
count_signpost_projected = """
WITH tmp AS (
    SELECT
        (SELECT count(*) FROM montreal_signpost_onroad) as a
        , (SELECT count(*) FROM montreal_signpost) as b
)
SELECT
    a::float / b * 100, b
FROM tmp
"""

# generate signposts orphans
generate_signposts_orphans = """
DROP TABLE IF EXISTS montreal_signposts_orphans;
CREATE TABLE montreal_signposts_orphans AS
(WITH tmp as (
    SELECT id FROM montreal_signpost
    EXCEPT
    SELECT id FROM montreal_signpost_onroad
) SELECT
    s.*
FROM tmp t
JOIN montreal_signpost s using(id)
)
"""

# create potential slots determined with signposts projected as start and end points
create_slots_likely = """
DROP TABLE IF EXISTS montreal_slots_likely;
CREATE TABLE montreal_slots_likely(
    id serial
    , signposts integer[]
    , rid integer  -- road id
    , position float
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
    FROM montreal_roads_geobase r, montreal_signpost_onroad p
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
    SELECT DISTINCT ON (rid, position)
        rid
        , position
        , rank() over (partition by rid order by position) as idx
        , signpost
    FROM point_list
)
INSERT INTO montreal_slots_likely (signposts, rid, position, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , w.id
    , loc1.position as position
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (rid)
JOIN montreal_roads_geobase w on w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""

create_nextpoints_for_signposts = """
DROP TABLE IF EXISTS montreal_nextpoints;
CREATE TABLE montreal_nextpoints AS
(WITH tmp as (
SELECT
    spo.id
    , sl.id as slot_id
    , spo.geom as spgeom
    , case
        when st_equals(
                ST_SnapToGrid(st_startpoint(sl.geom), 0.01),
                ST_SnapToGrid(spo.geom, 0.01)
            ) then st_pointN(sl.geom, 2)
        when st_equals(
                ST_SnapToGrid(st_endpoint(sl.geom), 0.01),
                ST_SnapToGrid(spo.geom, 0.01)
            ) then st_pointN(st_reverse(sl.geom), 2)
        else NULL
      end as geom
    , sp.geom as sgeom
FROM montreal_signpost_onroad spo
JOIN montreal_signpost sp on sp.id = spo.id
JOIN montreal_slots_likely sl on ARRAY[spo.id] <@ sl.signposts
) select
    id
    , slot_id
    , CASE  -- compute signed area to find if the nexpoint is on left or right
        WHEN
            sign((st_x(sgeom) - st_x(spgeom)) * (st_y(geom) - st_y(spgeom)) -
            (st_x(geom) - st_x(spgeom)) * (st_y(sgeom) - st_y(spgeom))) = 1 THEN 1 -- on left
        ELSE 2 -- right
        END as direction
    , geom
from tmp)
"""

insert_slots_temp = """
WITH tmp AS (
    -- select north and south from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM montreal_slots_likely sl
    JOIN montreal_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN montreal_signpost_onroad spo on s.signpost = spo.id
    JOIN montreal_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN montreal_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM montreal_slots_likely sl
    JOIN montreal_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN montreal_signpost_onroad spo on s.signpost = spo.id
    JOIN montreal_roads_geobase rb on spo.road_id = rb.id
),
selection as (
SELECT
    distinct on (t.id) t.id
    , min(signposts) as signposts
    , min(isleft) as isleft
    , min(rid) as rid
    , min(position) as position
    , min(name) as way_name
    , array_to_json(
        array_agg(distinct
        json_build_object(
            'code', t.code,
            'description', r.description,
            'address', name,
            'season_start', r.season_start,
            'season_end', r.season_end,
            'agenda', r.agenda,
            'time_max_parking', r.time_max_parking,
            'special_days', r.special_days,
            'restrict_typ', r.restrict_typ,
            'permit_no', z.number
        )::jsonb
    ))::jsonb as rules
    , CASE
        WHEN min(isleft) = 1 then
            ST_OffsetCurve(min(t.geom), {offset}, 'quad_segs=4 join=round')::geometry(linestring, 3857)
        ELSE
            ST_OffsetCurve(min(t.geom), -{offset}, 'quad_segs=4 join=round')::geometry(linestring, 3857)
      END as geom
FROM tmp t
JOIN rules r ON t.code = r.code
LEFT JOIN permit_zones z ON r.restrict_typ = 'permit' AND ST_Intersects(t.geom, z.geom)
GROUP BY t.id
) INSERT INTO montreal_slots_temp (rid, position, signposts, rules, geom, way_name)
SELECT
    rid
    , position
    , signposts
    , rules
    , geom
    , way_name
FROM selection
"""

overlay_paid_rules = """
WITH tmp AS (
    SELECT DISTINCT ON (foo.id)
        b.gid AS id,
        (b.rate / 100) AS rate,
        string_to_array(b.rules, ', ') AS rules,
        foo.id AS slot_id,
        foo.way_name,
        array_agg(foo.rules) AS orig_rules
    FROM montreal_bornes b, montreal_roads_geobase r,
        (
            SELECT id, rid, way_name, geom, jsonb_array_elements(rules) AS rules
            FROM montreal_slots_temp
            GROUP BY id
        ) foo
    WHERE r.id_trc = b.geobase_id
        AND r.id = foo.rid
        AND ST_DWithin(foo.geom, b.geom, 12)
    GROUP BY b.gid, b.geom, b.rate, b.rules, foo.id, foo.geom, foo.way_name
    ORDER BY foo.id, ST_Distance(foo.geom, b.geom)
), new_slots AS (
    SELECT t.slot_id, array_to_json(array_cat(t.orig_rules, array_agg(
        distinct json_build_object(
            'code', r.code,
            'description', r.description,
            'address', t.way_name,
            'season_start', r.season_start,
            'season_end', r.season_end,
            'agenda', r.agenda,
            'time_max_parking', r.time_max_parking,
            'special_days', r.special_days,
            'restrict_typ', r.restrict_typ,
            'paid_hourly_rate', t.rate
        )::jsonb)
    ))::jsonb AS rules
    FROM tmp t
    JOIN rules r ON r.code = ANY(t.rules)
    WHERE r.code NOT LIKE '%%MTLPAID-M%%'
    GROUP BY t.slot_id, t.orig_rules
)
UPDATE montreal_slots_temp s
SET rules = n.rules
FROM new_slots n
WHERE n.slot_id = s.id
"""

create_slots_for_debug = """
DROP TABLE IF EXISTS montreal_slots_debug;
CREATE TABLE montreal_slots_debug as
(
    WITH tmp as (
    -- select north and south from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM montreal_slots_likely sl
    JOIN montreal_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN montreal_signpost_onroad spo on s.signpost = spo.id
    JOIN montreal_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN montreal_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM montreal_slots_likely sl
    JOIN montreal_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN montreal_signpost_onroad spo on s.signpost = spo.id
    JOIN montreal_roads_geobase rb on spo.road_id = rb.id
)
SELECT
    distinct on (t.id, t.code)
    row_number() over () as pkid
    , t.id
    , t.code
    , t.signposts
    , t.isleft
    , t.name as way_name
    , rt.description
    , rt.season_start
    , rt.season_end
    , rt.time_max_parking
    , rt.time_start
    , rt.time_end
    , rt.time_duration
    , rt.lun
    , rt.mar
    , rt.mer
    , rt.jeu
    , rt.ven
    , rt.sam
    , rt.dim
    , rt.daily
    , rt.special_days
    , rt.restrict_typ
    , r.agenda::text as agenda
    , CASE
        WHEN isleft = 1 then
            ST_OffsetCurve(t.geom, {offset}, 'quad_segs=4 join=round')::geometry(linestring, 3857)
        ELSE
            ST_OffsetCurve(t.geom, -{offset}, 'quad_segs=4 join=round')::geometry(linestring, 3857)
      END as geom
FROM tmp t
JOIN rules r on t.code = r.code
JOIN montreal_rules_translation rt on rt.code = r.code
)
"""
