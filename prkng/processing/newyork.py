# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS newyork_sign;
CREATE TABLE newyork_sign (
    id serial PRIMARY KEY
    , sid integer NOT NULL
    , geom geometry(Point, 3857)
    , direction integer -- direction the rule applies (cardinal/intercardinal)
    , elevation integer -- higher is prioritary
    , code varchar -- code of rule
    , signpost integer
    , description varchar -- description of rule
)
"""

# insert new york signs
insert_sign = """
INSERT INTO newyork_sign
(
    sid
    , geom
    , elevation
    , code
    , description
    , direction
)
SELECT
    DISTINCT ON (p.sg_order_n, p.sg_mutcd_c, p.x, p.y)
    p.objectid::int
    , p.geom
    , p.sg_seqno_n::int
    , p.sg_mutcd_c
    , r.description
    , CASE left(l.sos, 1)
        WHEN 'N' THEN (CASE left(p.sg_arrow_d, 1) WHEN 'W' THEN 1 WHEN 'E' THEN 2 ELSE 0 END)
        WHEN 'S' THEN (CASE left(p.sg_arrow_d, 1) WHEN 'E' THEN 1 WHEN 'W' THEN 2 ELSE 0 END)
        WHEN 'E' THEN (CASE left(p.sg_arrow_d, 1) WHEN 'N' THEN 1 WHEN 'S' THEN 2 ELSE 0 END)
        WHEN 'W' THEN (CASE left(p.sg_arrow_d, 1) WHEN 'S' THEN 1 WHEN 'N' THEN 2 ELSE 0 END)
      END
FROM newyork_signs_raw p
JOIN rules r ON r.code = p.sg_mutcd_c -- only keep those existing in rules
JOIN newyork_roads_locations l ON p.sg_order_n = l.order_no
ORDER BY p.sg_order_n, p.sg_mutcd_c, p.x, p.y
"""

# create signpost table
create_signpost = """
DROP TABLE IF EXISTS newyork_signpost;
CREATE TABLE newyork_signpost (
    id serial PRIMARY KEY
    , boro varchar
    , geobase_id integer
    , order_no varchar
    , signs integer[]
    , geom geometry(Point, 3857)
);
"""

insert_signpost = """
INSERT INTO newyork_signpost (boro, order_no, signs, geom)
SELECT
    min(p.sg_key_bor),
    min(p.sg_order_n),
    array_agg(DISTINCT s.id),
    ST_SetSRID(ST_MakePoint(avg(ST_X(s.geom)), avg(ST_Y(s.geom))), 3857)
FROM newyork_sign s
JOIN newyork_signs_raw p ON p.objectid = s.sid
GROUP BY sg_order_n, sr_dist
"""


# try to match osm ways with geobase
match_roads_geobase = """
DROP TABLE IF EXISTS newyork_roads_geobase;
CREATE TABLE newyork_roads_geobase (
    id integer
    , osm_id bigint
    , boro varchar
    , name varchar
    , physicalid integer
    , b5sc integer
    , geom geometry(Linestring, 3857)
);

WITH tmp AS (
    SELECT
        o.*
        , m.b5sc::int
        , m.physicalid::int
        , (CASE m.boroughcod::int
            WHEN 1 THEN 'M' -- Manhattan
            WHEN 2 THEN 'B' -- The Bronx
            WHEN 3 THEN 'K' -- Brooklyn
            WHEN 4 THEN 'Q' -- Queens
            WHEN 5 THEN 'S' -- Staten Island
          END) AS boro
        , rank() OVER (
            PARTITION BY o.id ORDER BY
              ST_HausdorffDistance(o.geom, m.geom)
              , levenshtein(o.name, btrim(s.stname_lab))
              , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
          ) AS rank
    FROM roads o
    JOIN newyork_geobase m ON o.geom && ST_Expand(m.geom, 10)
    JOIN newyork_snd s ON m.boroughcod::int = s.boro AND m.b7sc::int = s.b7sc
    WHERE ST_Contains(ST_Buffer(m.geom, 30), o.geom)
)
INSERT INTO newyork_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , boro
    , name
    , physicalid
    , b5sc
    , geom
FROM tmp
WHERE rank = 1;


-- invert buffer comparison to catch more ways
WITH tmp AS (
      SELECT
          o.*
          , m.b5sc::int
          , m.physicalid::int
          , (CASE m.boroughcod::int
              WHEN 1 THEN 'M' -- Manhattan
              WHEN 2 THEN 'B' -- The Bronx
              WHEN 3 THEN 'K' -- Brooklyn
              WHEN 4 THEN 'Q' -- Queens
              WHEN 5 THEN 'S' -- Staten Island
            END) AS boro
          , rank() OVER (
              PARTITION BY o.id ORDER BY
                ST_HausdorffDistance(o.geom, m.geom)
                , levenshtein(o.name, btrim(s.stname_lab))
                , abs(ST_Length(o.geom) - ST_Length(m.geom)) / greatest(ST_Length(o.geom), ST_Length(m.geom))
            ) AS rank
      FROM roads o
      LEFT JOIN newyork_roads_geobase orig ON orig.id = o.id
      JOIN newyork_geobase m ON o.geom && ST_Expand(m.geom, 10)
      JOIN newyork_snd s ON m.boroughcod::int = s.boro AND m.b7sc::int = s.b7sc
      WHERE ST_Contains(ST_Buffer(o.geom, 30), m.geom)
        AND orig.id IS NULL
)
INSERT INTO newyork_roads_geobase
SELECT
    DISTINCT ON (id)
    id
    , osm_id
    , boro
    , name
    , physicalid
    , b5sc
    , geom
FROM tmp
WHERE rank = 1;
"""

match_signposts = """
WITH wsndname AS (
    SELECT
        g.id,
        g.boro,
        btrim(s.stname_lab) AS snd_name
    FROM newyork_roads_geobase g
    JOIN newyork_snd s ON g.b5sc::int = s.b5sc
), posts AS (
    SELECT
        DISTINCT ON (s.id)
        s.id AS sid,
        g.id AS gid
    FROM newyork_signpost s
    JOIN newyork_roads_locations l ON s.boro = l.boro AND s.order_no = l.order_no
    JOIN wsndname x ON btrim(split_part(l.main_st, '*', 1)) = x.snd_name
    JOIN newyork_roads_geobase g ON x.id = g.id
    ORDER BY s.id, ST_Distance(g.geom, s.geom)
)
UPDATE newyork_signpost s
SET geobase_id = p.gid
FROM posts p
WHERE s.id = p.sid
"""

# project signposts on road and
# determine if they were on the left side or right side of the road
project_signposts = """
DROP TABLE IF EXISTS newyork_signpost_onroad;
CREATE TABLE newyork_signpost_onroad AS
    SELECT
        DISTINCT ON (sp.id) sp.id  -- hack to prevent duplicata
        , s.id AS road_id
        , ST_ClosestPoint(s.geom, sp.geom)::geometry(point, 3857) AS geom
        , ST_isLeft(s.geom, sp.geom) AS isleft
    FROM newyork_signpost sp
    JOIN newyork_roads_geobase s ON sp.geobase_id = s.id
    WHERE sp.boro = 'K' -- FIXME
    ORDER BY sp.id, ST_Distance(s.geom, sp.geom);

SELECT id FROM newyork_signpost_onroad GROUP BY id HAVING count(*) > 1
"""

# how many signposts have been projected ?
count_signpost_projected = """
WITH tmp AS (
    SELECT
        (SELECT count(*) FROM newyork_signpost_onroad) as a
        , (SELECT count(*) FROM newyork_signpost) as b
)
SELECT
    a::float / b * 100, b
FROM tmp
"""

# generate signposts orphans
generate_signposts_orphans = """
DROP TABLE IF EXISTS newyork_signposts_orphans;
CREATE TABLE newyork_signposts_orphans AS
(WITH tmp as (
    SELECT id FROM newyork_signpost
    EXCEPT
    SELECT id FROM newyork_signpost_onroad
) SELECT
    s.*
FROM tmp t
JOIN newyork_signpost s USING (id)
)
"""

add_signposts_to_sign = """
WITH tmp AS (
    SELECT DISTINCT s.id AS sign_id, p.id AS post_id
    FROM newyork_sign s
    JOIN newyork_signpost p ON s.id = ANY(p.signs)
)
UPDATE newyork_sign s
SET signpost = tmp.post_id
FROM tmp
WHERE s.id = tmp.sign_id;
"""

# create potential slots determined with signposts projected as start and end points
create_slots_likely = """
DROP TABLE IF EXISTS newyork_slots_likely;
CREATE TABLE newyork_slots_likely(
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
    FROM newyork_roads_geobase r, newyork_signpost_onroad p
    WHERE r.geom && p.geom
        AND r.id = p.road_id
        AND p.isleft = {isleft}
), point_list AS (
    SELECT
        DISTINCT rid
        , 0 AS position
        , 0 AS signpost
    FROM selected_roads
UNION ALL
    SELECT
        DISTINCT rid
        , 1 AS position
        , 0 AS signpost
    FROM selected_roads
UNION ALL
    SELECT
        rid
        , ST_Line_Locate_Point(rgeom, pgeom) AS position
        , pid AS signpost
    FROM selected_roads
), loc_with_idx AS (
    SELECT DISTINCT ON (rid, position)
        rid
        , position
        , rank() OVER (PARTITION BY rid ORDER BY position) AS idx
        , signpost
    FROM point_list
)
INSERT INTO newyork_slots_likely (signposts, rid, position, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , w.id
    , loc1.position AS position
    , ST_Line_Substring(w.geom, loc1.position, loc2.position) AS geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 USING (rid)
JOIN newyork_roads_geobase w ON w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""

create_nextpoints_for_signposts = """
DROP TABLE IF EXISTS newyork_nextpoints;
CREATE TABLE newyork_nextpoints AS (
    WITH tmp as (
        SELECT
            spo.id
            , sl.id as slot_id
            , spo.geom as spgeom
            , case
                when ST_Equals(
                        ST_SnapToGrid(ST_StartPoint(sl.geom), 0.01),
                        ST_SnapToGrid(spo.geom, 0.01)
                    ) then ST_PointN(sl.geom, 2)
                when ST_Equals(
                        ST_SnapToGrid(ST_EndPoint(sl.geom), 0.01),
                        ST_SnapToGrid(spo.geom, 0.01)
                    ) then ST_PointN(ST_Reverse(sl.geom), 2)
                else NULL
              end as geom
            , sp.geom as sgeom
        FROM newyork_signpost_onroad spo
        JOIN newyork_signpost sp ON sp.id = spo.id
        JOIN newyork_slots_likely sl ON ARRAY[spo.id] <@ sl.signposts
    )
    SELECT
        id
        , slot_id
        , CASE  -- compute signed area to find if the nexpoint is on left or right
            WHEN
                sign((st_x(sgeom) - st_x(spgeom)) * (st_y(geom) - st_y(spgeom)) -
                (st_x(geom) - st_x(spgeom)) * (st_y(sgeom) - st_y(spgeom))) = 1 THEN 1 -- on left
            ELSE 2 -- right
            END AS direction
        , geom
    FROM tmp
)
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
    FROM newyork_slots_likely sl
    JOIN newyork_sign s ON ARRAY[s.signpost] <@ sl.signposts
    JOIN newyork_signpost_onroad spo ON s.signpost = spo.id
    JOIN newyork_nextpoints np ON np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN newyork_roads_geobase rb ON spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM newyork_slots_likely sl
    JOIN newyork_sign s ON ARRAY[s.signpost] <@ sl.signposts AND direction = 0
    JOIN newyork_signpost_onroad spo ON s.signpost = spo.id
    JOIN newyork_roads_geobase rb ON spo.road_id = rb.id
), selection AS (
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
            'metered', r.metered,
            'restrict_typ', r.restrict_typ,
            'permit_no', (CASE WHEN r.permit_no = '' THEN NULL ELSE r.permit_no END)
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
GROUP BY t.id
) INSERT INTO newyork_slots_temp (rid, position, signposts, rules, geom, way_name)
SELECT
    rid
    , position
    , signposts
    , rules
    , geom
    , way_name
FROM selection
"""


create_slots_for_debug = """
DROP TABLE IF EXISTS newyork_slots_debug;
CREATE TABLE newyork_slots_debug as
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
    FROM newyork_slots_likely sl
    JOIN newyork_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN newyork_signpost_onroad spo on s.signpost = spo.id
    JOIN newyork_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN newyork_roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM newyork_slots_likely sl
    JOIN newyork_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN newyork_signpost_onroad spo on s.signpost = spo.id
    JOIN newyork_roads_geobase rb on spo.road_id = rb.id
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
    , rt.metered
    , rt.restrict_typ
    , rt.permit_no
    , r.agenda::text as agenda
    , CASE
        WHEN isleft = 1 then
            ST_OffsetCurve(t.geom, {offset}, 'quad_segs=4 join=round')::geometry(linestring, 3857)
        ELSE
            ST_OffsetCurve(t.geom, -{offset}, 'quad_segs=4 join=round')::geometry(linestring, 3857)
      END as geom
FROM tmp t
JOIN rules r on t.code = r.code
JOIN newyork_rules_translation rt on rt.code = r.code
)
"""
