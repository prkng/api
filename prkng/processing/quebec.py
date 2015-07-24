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

create_bornes_raw = """
DROP TABLE IF EXISTS quebec_bornes_raw;
CREATE TABLE quebec_bornes_raw (
    id serial PRIMARY KEY,
    no_borne integer,
    nom_topog varchar,
    isleft integer,
    geom geometry,
    road_id integer,
    road_pos float
)
"""

# insert quebec virtual signposts for paid slots
create_paid_signpost = """
WITH bornes AS (
    SELECT
        b.*,
        st_azimuth(st_closestpoint(r.geom, b.geom)::geometry(point, 3857), b.geom) AS azi,
        rank() over (
            PARTITION BY b.id
            ORDER BY levenshtein(b.nom_topog, r.name),
                st_distance(b.geom, r.geom)
        ) AS rank,
        r.id AS road_id,
        r.geom AS geom_road
    FROM quebec_bornes b
    JOIN roads r on r.geom && st_buffer(b.geom, 30)
    WHERE nom_topog NOT LIKE '%Stationnement%'
      AND no_borne::int NOT BETWEEN 6100 AND 6125   -- exclude mis-identified lot
), bornes_proj AS (
    SELECT
        s.no_borne::int,
        s.nom_topog,
        s.road_id,
        s.geom_road,
        ST_isLeft(s.geom_road, s.geom) AS isleft,
        CASE WHEN (s.azi - radians(90.0) > 2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.geom_road, s.geom), 4326)::geography, 3, (s.azi - radians(90.0) - (2*pi())))::geometry, 3857)
        WHEN (s.azi - radians(90.0) < -2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.geom_road, s.geom), 4326)::geography, 3, (s.azi - radians(90.0) + (2*pi())))::geometry, 3857)
        ELSE
            st_transform(st_project(st_transform(st_closestpoint(s.geom_road, s.geom), 4326)::geography, 3, s.azi - radians(90.0))::geometry, 3857)
        END AS geom
    FROM bornes s
    WHERE s.rank = 1
    UNION ALL
    SELECT
        s.no_borne::int,
        s.nom_topog,
        s.road_id,
        s.geom_road,
        ST_isLeft(s.geom_road, s.geom) AS isleft,
        CASE WHEN (s.azi + radians(90.0) > 2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.geom_road, s.geom), 4326)::geography, 3, (s.azi + radians(90.0) - (2*pi())))::geometry, 3857)
        WHEN (s.azi + radians(90.0) < -2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.geom_road, s.geom), 4326)::geography, 3, (s.azi + radians(90.0) + (2*pi())))::geometry, 3857)
        ELSE
            st_transform(st_project(st_transform(st_closestpoint(s.geom_road, s.geom), 4326)::geography, 3, s.azi + radians(90.0))::geometry, 3857)
        END AS geom
    FROM bornes s
    WHERE s.rank = 1
)
INSERT INTO quebec_bornes_raw (no_borne, nom_topog, isleft, geom, road_id, road_pos)
    SELECT
        s.no_borne::int,
        s.nom_topog,
        s.isleft,
        s.geom,
        s.road_id,
        st_line_locate_point(s.geom_road, s.geom)
    FROM bornes_proj s
"""

aggregate_paid_signposts = """
DO
$$
DECLARE
  borne record;
  id_a integer;
  id_b integer;
  id_match integer;
BEGIN
  DROP TABLE IF EXISTS quebec_bornes_clustered;
  CREATE TABLE quebec_bornes_clustered (id serial primary key, ids integer[], bornes integer[], geom geometry, way_name varchar, isleft integer, road_id integer);
  DROP TABLE IF EXISTS quebec_paid_slots_raw;
  CREATE TABLE quebec_paid_slots_raw (id serial primary key, road_id integer, bornes integer[], geom geometry, isleft integer);
  CREATE INDEX ON quebec_paid_slots_raw USING GIST(geom);

  FOR borne IN SELECT * FROM quebec_bornes_raw ORDER BY road_id, road_pos LOOP
    SELECT id FROM quebec_bornes_clustered
      WHERE borne.road_id = quebec_bornes_clustered.road_id
      AND borne.isleft = quebec_bornes_clustered.isleft
      AND ST_DWithin(borne.geom, quebec_bornes_clustered.geom, 10)
      LIMIT 1 INTO id_match;

    IF id_match IS NULL THEN
      INSERT INTO quebec_bornes_clustered (ids, bornes, geom, way_name, isleft, road_id) VALUES
        (ARRAY[borne.id], ARRAY[borne.no_borne], borne.geom, borne.nom_topog, borne.isleft, borne.road_id);
    ELSE
      UPDATE quebec_bornes_clustered SET geom = ST_MakeLine(borne.geom, geom),
        ids = uniq(sort(array_prepend(borne.id, ids))), bornes = uniq(sort(array_prepend(borne.no_borne, bornes)))
      WHERE quebec_bornes_clustered.id = id_match;
    END IF;
  END LOOP;

  WITH tmp_slots as (
    SELECT
      road_id,
      bornes,
      isleft,
      ST_Line_Locate_Point(r.geom, ST_StartPoint(qbc.geom)) AS start,
      ST_Line_Locate_Point(r.geom, ST_EndPoint(qbc.geom)) AS end
    FROM quebec_bornes_clustered qbc
    JOIN roads r ON r.id = qbc.road_id
  )
  INSERT INTO quebec_paid_slots_raw (road_id, geom, bornes, isleft)
    SELECT
      r.id,
      CASE
          WHEN isleft = 1 then
              ST_OffsetCurve(ST_Line_Substring(r.geom, LEAST(s.start, s.end), GREATEST(s.start, s.end)), {offset}, 'quad_segs=4 join=round')
          ELSE
              ST_OffsetCurve(ST_Line_Substring(r.geom, LEAST(s.start, s.end), GREATEST(s.start, s.end)), -{offset}, 'quad_segs=4 join=round')
      END AS geom,
      s.bornes,
      s.isleft
    FROM tmp_slots s
    JOIN roads r ON r.id = s.road_id;
END;
$$ language plpgsql;
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
) INSERT INTO slots (signposts, rules, geom, way_name)
SELECT
    signposts
    , rules
    , geom::geometry(linestring, 3857)
    , way_name
FROM selection
WHERE st_geometrytype(geom) = 'ST_LineString' -- skip curious rings
"""

overlay_paid_rules = """
WITH segments AS (
    SELECT
        id, ST_Intersection(geom, exclude) AS geom_paid, ST_Difference(geom, exclude) AS geom_normal,
        exclude, signposts, way_name, orig_rules, array_agg(rules) AS rules
    FROM (
        SELECT
            s.id, s.geom, s.signposts, s.way_name, s.rules AS orig_rules,
            jsonb_array_elements(s.rules) AS rules,
            ST_Union(ST_Buffer(qps.geom, 1, 'endcap=flat join=round')) AS exclude
        FROM slots s
        JOIN quebec_paid_slots_raw qps ON ST_Intersects(s.geom, ST_Buffer(qps.geom, 1, 'endcap=flat join=round'))
        JOIN roads r ON r.id = qps.road_id AND s.way_name = r.name
        GROUP BY s.id
    ) AS foo
    GROUP BY id, geom, exclude, signposts, way_name, orig_rules
    ORDER BY id
), update_normal AS (
    DELETE FROM slots
    USING segments
    WHERE slots.id = segments.id
), new_paids AS (
    INSERT INTO slots (signposts, rules, way_name, geom)
        SELECT
            g.signposts,
            array_to_json(array_append(g.rules,
                json_build_object(
                    'code', z.code,
                    'description', z.description,
                    'address', g.way_name,
                    'season_start', z.season_start,
                    'season_end', z.season_end,
                    'agenda', z.agenda,
                    'time_max_parking', z.time_max_parking,
                    'special_days', z.special_days,
                    'restrict_typ', z.restrict_typ,
                    'paid_hourly_rate', 2.25
                )::jsonb)
            )::jsonb,
            g.way_name,
            CASE ST_GeometryType(g.geom_paid)
                WHEN 'ST_LineString' THEN
                    g.geom_paid
                ELSE
                    (ST_Dump(g.geom_paid)).geom
            END
        FROM segments g
        JOIN rules z ON z.code = 'QCPAID'
), new_normals AS (
    SELECT
        g.id,
        g.signposts,
        g.way_name,
        g.orig_rules,
        CASE ST_GeometryType(g.geom_normal)
            WHEN 'ST_LineString' THEN
                g.geom_normal
            ELSE
                (ST_Dump(g.geom_normal)).geom
        END AS geom
    FROM segments g
)
INSERT INTO slots (signposts, rules, way_name, geom)
    SELECT
        nn.signposts,
        nn.orig_rules,
        nn.way_name,
        nn.geom
    FROM new_normals nn
    WHERE ST_Length(nn.geom) >= 3
"""

create_paid_slots_standalone = """
WITH exclusions AS (
    SELECT
        id, way_name, ST_Union(exclude) AS exclude
    FROM (
        SELECT
            qps.id,
            r.name AS way_name,
            ST_Buffer(s.geom, 1, 'endcap=flat join=round') AS exclude
        FROM quebec_paid_slots_raw qps
        JOIN slots s ON ST_Intersects(s.geom, ST_Buffer(qps.geom, 1, 'endcap=flat join=round'))
        JOIN roads r ON r.id = qps.road_id AND s.way_name = r.name
    ) AS foo
    GROUP BY id, way_name
), update_raw AS (
    SELECT
        qps.id,
        ex.way_name,
        ST_Difference(qps.geom, ex.exclude) AS geom
    FROM quebec_paid_slots_raw qps
    JOIN exclusions ex ON ex.id = qps.id
    UNION
    SELECT
        qps.id,
        r.name,
        qps.geom
    FROM quebec_paid_slots_raw qps
    JOIN roads r ON r.id = qps.road_id
    WHERE qps.id NOT IN (SELECT id FROM exclusions)
), new_paid AS (
    SELECT
        ur.way_name,
        array_to_json(
            array[json_build_object(
                'code', z.code,
                'description', z.description,
                'address', ur.way_name,
                'season_start', z.season_start,
                'season_end', z.season_end,
                'agenda', z.agenda,
                'time_max_parking', z.time_max_parking,
                'special_days', z.special_days,
                'restrict_typ', z.restrict_typ,
                'paid_hourly_rate', 2.25
            )::jsonb]
        )::jsonb AS rules,
        CASE ST_GeometryType(ur.geom)
            WHEN 'ST_LineString' THEN
                ur.geom
            ELSE
                (ST_Dump(ur.geom)).geom
        END AS geom
    FROM update_raw ur
    JOIN rules z ON z.code = 'QCPAID'
)
INSERT INTO slots (signposts, rules, way_name, geom)
    SELECT
        ARRAY[0,0],
        nn.rules,
        nn.way_name,
        nn.geom
    FROM new_paid nn
    WHERE ST_Length(nn.geom) >= 3
"""

create_client_data = """
UPDATE slots SET
    geojson = ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
    button_location = json_build_object('long', ST_X(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)),
        'lat', ST_Y(ST_Transform(ST_Line_Interpolate_Point(geom, 0.5), 4326)))::jsonb
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
