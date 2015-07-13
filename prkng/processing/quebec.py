# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS quebec_sign;
CREATE TABLE quebec_sign (
    id integer PRIMARY KEY
    , geom geometry(Point, 3857)
    , direction smallint -- direction the rule applies (0: both side, 1: left, 2: right)
    , code varchar -- code of rule
    , signpost integer
    , description varchar -- description of rule
)
"""

# insert quebec signs
insert_sign = """
INSERT INTO quebec_sign
(
    id
    , geom
    , direction
    , code
    , description
)
SELECT
    distinct on (id)
    p.id::int
    , p.geom
    , case
        when substring(r.description, '.*\((fl.*dr.*)\).*') is not NULL then 2 -- right
        when substring(r.description, '.*\((fl.*ga.*)\).*') is not NULL then 1 -- left
        when substring(r.description, '.*\((fl.*do.*)\).*') is not NULL then 0 -- both sides
        else 0 -- consider both side if no information related to direction
      end as direction
    , p.type_code
    , p.type_desc
FROM quebec_panneau p
JOIN rules r on r.code = p.type_code -- only keep those existing in rules
WHERE p.type_desc not ilike '%panonceau%' -- exclude panonceau
ORDER BY id, gid
"""


# creating signposts (aggregation of signs sharing the same lect_met attribute)
create_signpost = """
DROP TABLE IF EXISTS quebec_signpost;
CREATE TABLE quebec_signpost (
    id serial PRIMARY KEY
    , rid integer  -- road id from roads table
    , signs integer[]
    , geom geometry(Point, 3857)
);

WITH tmp as (
SELECT
    min(s.id) as id
    , st_setsrid(st_makepoint(avg(st_x(s.geom)), avg(st_y(s.geom))), 3857) as geom
    , min(p.nom_topog) as nom_topog
    , array_agg(distinct s.id) as ids
FROM quebec_sign s
JOIN quebec_panneau p on p.id = s.id
GROUP BY lect_met, id_voie_pu, cote_rue
), ranked as (
select
    r.id as rid
    , t.geom
    , ids
    , st_distance(t.geom, r.geom) as dist
    , rank() over (
        partition by t.id order by levenshtein(t.nom_topog, r.name), st_distance(t.geom, r.geom)
        ) as rank
  from tmp t
  JOIN roads r on r.geom && st_buffer(t.geom, 30)
)
INSERT INTO quebec_signpost
SELECT
    distinct on (dist, rank, ids)
    row_number() over () as id
    , rid
    , ids
    , geom
FROM ranked WHERE rank = 1
"""

# project signposts on road and
# determine if they were on the left side or right side of the road
project_signposts = """
DROP TABLE IF EXISTS quebec_signpost_onroad;
CREATE TABLE quebec_signpost_onroad AS
    SELECT
        distinct on (sp.id) sp.id
        , s.id as road_id
        , st_closestpoint(s.geom, sp.geom)::geometry(point, 3857) as geom
        , st_isleft(s.geom, sp.geom) as isleft
    FROM quebec_signpost sp
    JOIN roads s on s.id = sp.rid;

SELECT id from quebec_signpost_onroad group by id having count(*) > 1
"""


# how many signposts have been projected ?
count_signpost_projected = """
WITH tmp AS (
    SELECT
        (SELECT count(*) FROM quebec_signpost_onroad) as a
        , (SELECT count(*) FROM quebec_signpost) as b
)
SELECT
    a::float / b * 100, b
FROM tmp
"""

# generate signposts orphans
generate_signposts_orphans = """
DROP TABLE IF EXISTS quebec_signposts_orphans;
CREATE TABLE quebec_signposts_orphans AS
(WITH tmp as (
    SELECT id FROM quebec_signpost
    EXCEPT
    SELECT id FROM quebec_signpost_onroad
) SELECT
    s.*
FROM tmp t
JOIN quebec_signpost s using(id)
)
"""

add_signposts_to_sign = """
UPDATE quebec_sign
SET signpost = (select distinct id from quebec_signpost where ARRAY[quebec_sign.id] <@ signs)
"""

# create potential slots determined with signposts projected as start and end points
create_slots_likely = """
DROP TABLE IF EXISTS quebec_slots_likely;
CREATE TABLE quebec_slots_likely(
    id serial
    , signposts integer[]
    , rid integer  -- road id
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
    FROM roads r, quebec_signpost_onroad p
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
INSERT INTO quebec_slots_likely (signposts, rid, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , w.id
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (rid)
JOIN roads w on w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""

create_nextpoints_for_signposts = """
DROP TABLE IF EXISTS quebec_nextpoints;
CREATE TABLE quebec_nextpoints AS
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
FROM quebec_signpost_onroad spo
JOIN quebec_signpost sp on sp.id = spo.id
JOIN quebec_slots_likely sl on ARRAY[spo.id] <@ sl.signposts
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

insert_slots = """
WITH tmp AS (
    -- select north and south from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM quebec_slots_likely sl
    JOIN quebec_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN quebec_signpost_onroad spo on s.signpost = spo.id
    JOIN quebec_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN roads rb on spo.road_id = rb.id


    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM quebec_slots_likely sl
    JOIN quebec_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN quebec_signpost_onroad spo on s.signpost = spo.id
    JOIN roads rb on spo.road_id = rb.id

),
selection as (
SELECT
    distinct on (t.id) t.id
    , min(signposts) as signposts
    , min(isleft) as isleft
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
            'permit_no', r.permit_no
        )::jsonb
    ))::jsonb as rules
    , CASE
        WHEN min(isleft) = 1 then
            ST_OffsetCurve(min(t.geom), {offset}, 'quad_segs=4 join=round')
        ELSE
            ST_OffsetCurve(min(t.geom), -{offset}, 'quad_segs=4 join=round')
      END as geom
FROM tmp t
JOIN rules r on t.code = r.code
GROUP BY t.id
) INSERT INTO slots (signposts, rules, geom, geojson, button_location, way_name)
SELECT
    signposts
    , rules
    , geom::geometry(linestring, 3857)
    , ST_AsGeoJSON(st_transform(geom, 4326))::jsonb as geojson
    , json_build_object('long', st_x(center), 'lat', st_y(center))::jsonb
    , way_name
FROM selection,
LATERAL st_transform(ST_Line_Interpolate_Point(geom, 0.5), 4326) as center
WHERE st_geometrytype(geom) = 'ST_LineString' -- skip curious rings
"""

create_slots_for_debug = """
DROP TABLE IF EXISTS quebec_slots_debug;
CREATE TABLE quebec_slots_debug as
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
    FROM quebec_slots_likely sl
    JOIN quebec_sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN quebec_signpost_onroad spo on s.signpost = spo.id
    JOIN quebec_nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN roads rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM quebec_slots_likely sl
    JOIN quebec_sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN quebec_signpost_onroad spo on s.signpost = spo.id
    JOIN roads rb on spo.road_id = rb.id
), staging as (
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
            ST_OffsetCurve(t.geom, {offset}, 'quad_segs=4 join=round')
        ELSE
            ST_OffsetCurve(t.geom, -{offset}, 'quad_segs=4 join=round')
      END as geom
FROM tmp t
JOIN rules r on t.code = r.code
JOIN quebec_rules_translation rt on rt.code = r.code
) select * from staging
WHERE st_geometrytype(geom) = 'ST_LineString' -- skip curious rings
)
"""
