# -*- coding: utf-8 -*-
from __future__ import unicode_literals


# create table hosting all signs
create_sign = """
DROP TABLE IF EXISTS sign;
CREATE TABLE sign (
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

create_bornes_raw = """
DROP TABLE IF EXISTS montreal_bornes_raw;
CREATE TABLE montreal_bornes_raw (
    id serial PRIMARY KEY,
    no_borne varchar,
    nom_topog varchar,
    isleft integer,
    geom geometry,
    rules varchar,
    rate integer,
    road_id integer,
    road_pos float
)
"""

# insert montreal signs with associated postsigns
# only treat fleche_pan 0, 2, 3 for direction
# don't know what others mean
insert_sign = """
INSERT INTO sign
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
DROP TABLE IF EXISTS signpost;
CREATE TABLE signpost (
    id integer PRIMARY KEY
    , geobase_id integer
    , geom geometry(Point, 3857)
)
"""

# insert only signpost that have signs on it
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

# project signposts on road and
# determine if they were on the left side or right side of the road
project_signposts = """
DROP TABLE IF EXISTS signpost_onroad;
CREATE TABLE signpost_onroad AS
    SELECT
        distinct on (sp.id) sp.id  -- hack to prevent duplicata, FIXME
        , s.id as road_id
        , st_closestpoint(s.geom, sp.geom)::geometry(point, 3857) as geom
        , st_isleft(s.geom, sp.geom) as isleft
    FROM signpost sp
    JOIN roads_geobase s on s.id_trc = sp.geobase_id
    ORDER BY sp.id, ST_Distance(s.geom, sp.geom);

SELECT id from signpost_onroad group by id having count(*) > 1
"""

# how many signposts have been projected ?
count_signpost_projected = """
WITH tmp AS (
    SELECT
        (SELECT count(*) FROM signpost_onroad) as a
        , (SELECT count(*) FROM signpost) as b
)
SELECT
    a::float / b * 100, b
FROM tmp
"""

# generate signposts orphans
generate_signposts_orphans = """
DROP TABLE IF EXISTS signposts_orphans;
CREATE TABLE signposts_orphans AS
(WITH tmp as (
    SELECT id FROM signpost
    EXCEPT
    SELECT id FROM signpost_onroad
) SELECT
    s.*
FROM tmp t
JOIN signpost s using(id)
)
"""

# insert montreal virtual signposts for paid slots
create_paid_signpost = """
WITH bornes AS (
    SELECT
        b.*,
        st_azimuth(st_closestpoint(r.geom, b.geom)::geometry(point, 3857), b.geom) AS azi,
        r.id AS road_id,
        r.geom AS road_geom,
        r.name AS nom_topog
    FROM montreal_bornes b
    JOIN roads_geobase r ON b.geobase_id = r.id_trc
), bornes_proj AS (
    SELECT
        s.no_borne,
        s.nom_topog,
        s.rules,
        s.rate,
        s.road_id,
        s.road_geom,
        ST_isLeft(s.road_geom, s.geom) AS isleft,
        CASE WHEN (s.azi - radians(90.0) > 2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.road_geom, s.geom), 4326)::geography, 5, (s.azi - radians(90.0) - (2*pi())))::geometry, 3857)
        WHEN (s.azi - radians(90.0) < -2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.road_geom, s.geom), 4326)::geography, 5, (s.azi - radians(90.0) + (2*pi())))::geometry, 3857)
        ELSE
            st_transform(st_project(st_transform(st_closestpoint(s.road_geom, s.geom), 4326)::geography, 5, s.azi - radians(90.0))::geometry, 3857)
        END AS geom
    FROM bornes s
    UNION ALL
    SELECT
        s.no_borne,
        s.nom_topog,
        s.rules,
        s.rate,
        s.road_id,
        s.road_geom,
        ST_isLeft(s.road_geom, s.geom) AS isleft,
        CASE WHEN (s.azi + radians(90.0) > 2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.road_geom, s.geom), 4326)::geography, 5, (s.azi + radians(90.0) - (2*pi())))::geometry, 3857)
        WHEN (s.azi + radians(90.0) < -2*pi()) THEN
            st_transform(st_project(st_transform(st_closestpoint(s.road_geom, s.geom), 4326)::geography, 5, (s.azi + radians(90.0) + (2*pi())))::geometry, 3857)
        ELSE
            st_transform(st_project(st_transform(st_closestpoint(s.road_geom, s.geom), 4326)::geography, 5, s.azi + radians(90.0))::geometry, 3857)
        END AS geom
    FROM bornes s
)
INSERT INTO montreal_bornes_raw (no_borne, nom_topog, rules, rate, isleft, geom, road_id, road_pos)
    SELECT
        s.no_borne,
        s.nom_topog,
        s.rules,
        s.rate,
        s.isleft,
        s.geom,
        s.road_id,
        st_line_locate_point(s.road_geom, s.geom)
    FROM bornes_proj s
"""

aggregate_paid_signposts = """
DO
$$
DECLARE
  borne record;
  id_match integer;
BEGIN
  DROP TABLE IF EXISTS montreal_bornes_clustered;
  CREATE TABLE montreal_bornes_clustered (id serial primary key, ids integer[], bornes varchar[], rules varchar, rate integer, geom geometry, way_name varchar, isleft integer, road_id integer);
  DROP TABLE IF EXISTS montreal_paid_slots_raw;
  CREATE TABLE montreal_paid_slots_raw (id serial primary key, road_id integer, bornes varchar[], rules varchar[], rate float, geom geometry, isleft integer);
  CREATE INDEX ON montreal_paid_slots_raw USING GIST(geom);

  FOR borne IN SELECT * FROM montreal_bornes_raw ORDER BY road_id, road_pos LOOP
    SELECT id FROM montreal_bornes_clustered
      WHERE borne.road_id = montreal_bornes_clustered.road_id
      AND borne.isleft = montreal_bornes_clustered.isleft
      AND borne.rules = montreal_bornes_clustered.rules
      AND borne.rate = montreal_bornes_clustered.rate
      AND ST_DWithin(borne.geom, montreal_bornes_clustered.geom, 15)
    LIMIT 1 INTO id_match;

    IF id_match IS NULL THEN
      INSERT INTO montreal_bornes_clustered (ids, bornes, rules, rate, geom, way_name, isleft, road_id) VALUES
        (ARRAY[borne.id], ARRAY[borne.no_borne], borne.rules, borne.rate, borne.geom, borne.nom_topog, borne.isleft, borne.road_id);
    ELSE
      UPDATE montreal_bornes_clustered SET geom = ST_MakeLine(borne.geom, geom),
        ids = (CASE WHEN borne.id = ANY(ids) THEN ids ELSE array_prepend(borne.id, ids) END),
        bornes = (CASE WHEN borne.no_borne = ANY(bornes) THEN bornes ELSE array_prepend(borne.no_borne, bornes) END)
      WHERE montreal_bornes_clustered.id = id_match;
    END IF;
  END LOOP;

  WITH tmp_slots as (
    SELECT
      road_id,
      bornes,
      isleft,
      rules,
      rate,
      ST_Line_Locate_Point(r.geom, ST_StartPoint(mtl.geom)) AS start,
      ST_Line_Locate_Point(r.geom, ST_EndPoint(mtl.geom)) AS end
    FROM montreal_bornes_clustered mtl
    JOIN roads_geobase r ON r.id = mtl.road_id
  )
  INSERT INTO montreal_paid_slots_raw (road_id, geom, bornes, rules, rate, isleft)
    SELECT
      r.id,
      CASE
          WHEN isleft = 1 then
              ST_OffsetCurve(ST_Line_Substring(r.geom, LEAST(s.start, s.end), GREATEST(s.start, s.end)), {offset}, 'quad_segs=4 join=round')
          ELSE
              ST_OffsetCurve(ST_Line_Substring(r.geom, LEAST(s.start, s.end), GREATEST(s.start, s.end)), -{offset}, 'quad_segs=4 join=round')
      END AS geom,
      s.bornes,
      string_to_array(s.rules, ', '),
      (s.rate::float / 100),
      s.isleft
    FROM tmp_slots s
    JOIN roads r ON r.id = s.road_id;
END;
$$ language plpgsql;
"""

# create potential slots determined with signposts projected as start and end points
create_slots_likely = """
DROP TABLE IF EXISTS slots_likely;
CREATE TABLE slots_likely(
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
    SELECT DISTINCT ON (rid, position)
        rid
        , position
        , rank() over (partition by rid order by position) as idx
        , signpost
    FROM point_list
)
INSERT INTO slots_likely (signposts, rid, position, geom)
SELECT
    ARRAY[loc1.signpost, loc2.signpost]
    , w.id
    , loc1.position as position
    , st_line_substring(w.geom, loc1.position, loc2.position) as geom
FROM loc_with_idx loc1
JOIN loc_with_idx loc2 using (rid)
JOIN roads_geobase w on w.id = loc1.rid
WHERE loc2.idx = loc1.idx+1;
"""

create_nextpoints_for_signposts = """
DROP TABLE IF EXISTS nextpoints;
CREATE TABLE nextpoints AS
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
FROM signpost_onroad spo
JOIN signpost sp on sp.id = spo.id
JOIN slots_likely sl on ARRAY[spo.id] <@ sl.signposts
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
    FROM slots_likely sl
    JOIN sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN signpost_onroad spo on s.signpost = spo.id
    JOIN nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM slots_likely sl
    JOIN sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN signpost_onroad spo on s.signpost = spo.id
    JOIN roads_geobase rb on spo.road_id = rb.id
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
) INSERT INTO slots_temp (rid, position, signposts, rules, geom, way_name)
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
WITH segments AS (
    SELECT
        id, rid, rgeom, ST_Intersection(geom, exclude) AS geom_paid, ST_Difference(geom, exclude) AS geom_normal,
        exclude, signposts, way_name, orig_rules, array_agg(orig_rules_arr) AS orig_rules_arr, paid_rules, rate
    FROM (
        SELECT
            s.id, r.id AS rid, r.geom AS rgeom, s.geom, s.signposts, s.way_name, s.rules AS orig_rules,
            jsonb_array_elements(s.rules) AS orig_rules_arr,
            ST_Union(ST_Buffer(mps.geom, 1, 'endcap=flat join=round')) AS exclude,
            mps.rules AS paid_rules, mps.rate
        FROM slots_temp s
        JOIN montreal_paid_slots_raw mps ON ST_Intersects(s.geom, ST_Buffer(mps.geom, 1, 'endcap=flat join=round'))
        JOIN roads_geobase r ON r.id = mps.road_id AND s.way_name = r.name
        GROUP BY s.id, r.id, r.geom
    ) AS foo
    WHERE ST_Length(ST_Intersection(geom, exclude)) >= 4
    GROUP BY id, rid, rgeom, geom, exclude, signposts, way_name, orig_rules
    ORDER BY id
), update_normal AS (
    DELETE FROM slots_temp
    USING segments
    WHERE slots_temp.id = segments.id
), new_slots AS (
    SELECT
        g.signposts,
        g.rid,
        g.rgeom,
        g.way_name,
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
                'paid_hourly_rate', g.rate
            )::jsonb)
        )::jsonb AS rules,
        CASE ST_GeometryType(g.geom_paid)
            WHEN 'ST_LineString' THEN
                g.geom_paid
            ELSE
                (ST_Dump(g.geom_paid)).geom
        END AS geom
    FROM segments g
    JOIN rules z ON z.code = ANY(regexp_split_to_array(g.paid_rules, ', '))
    UNION
    SELECT
        g.signposts,
        g.rid,
        g.rgeom,
        g.way_name,
        g.orig_rules AS rules,
        CASE ST_GeometryType(g.geom_normal)
            WHEN 'ST_LineString' THEN
                g.geom_normal
            ELSE
                (ST_Dump(g.geom_normal)).geom
        END AS geom
    FROM segments g
)
INSERT INTO slots_temp (signposts, rid, position, rules, way_name, geom)
    SELECT
        nn.signposts,
        nn.rid,
        st_line_locate_point(nn.rgeom, st_startpoint(nn.geom)),
        nn.rules,
        nn.way_name,
        nn.geom
    FROM new_slots nn
    WHERE ST_Length(nn.geom) >= 4
"""

create_paid_slots_standalone = """
WITH exclusions AS (
    SELECT
        id, rid, rgeom, way_name, ST_Union(exclude) AS exclude
    FROM (
        SELECT
            mps.id,
            r.id AS rid,
            r.name AS way_name,
            r.geom AS rgeom,
            mps.rules,
            mps.rate,
            ST_Buffer(s.geom, 1, 'endcap=flat join=round') AS exclude
        FROM montreal_paid_slots_raw mps
        JOIN slots_temp s ON ST_Intersects(s.geom, ST_Buffer(mps.geom, 1, 'endcap=flat join=round'))
        JOIN roads_geobase r ON r.id = mps.road_id AND s.way_name = r.name
    ) AS foo
    GROUP BY id, rid, rgeom, way_name
), update_raw AS (
    SELECT
        ex.rid,
        ex.rgeom,
        ex.way_name,
        ex.rules,
        ex.rate,
        ST_Difference(mps.geom, ex.exclude) AS geom
    FROM montreal_paid_slots_raw mps
    JOIN exclusions ex ON ex.id = mps.id
    UNION
    SELECT
        r.id AS rid,
        r.geom AS rgeom,
        r.name,
        mps.rules,
        mps.rate,
        mps.geom
    FROM montreal_paid_slots_raw mps
    JOIN roads_geobase r ON r.id = mps.road_id
    WHERE mps.id NOT IN (SELECT id FROM exclusions)
), new_paid AS (
    SELECT
        ur.rid,
        ur.way_name,
        ur.rgeom,
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
                'paid_hourly_rate', ur.rate
            )::jsonb]
        )::jsonb AS rules,
        CASE ST_GeometryType(ur.geom)
            WHEN 'ST_LineString' THEN
                ur.geom
            ELSE
                (ST_Dump(ur.geom)).geom
        END AS geom
    FROM update_raw ur
    JOIN rules z ON z.code = ANY(regexp_split_to_array(ur.rules, ', '))
)
INSERT INTO slots_temp (signposts, rid, position, rules, way_name, geom)
    SELECT
        ARRAY[0,0],
        nn.rid,
        st_line_locate_point(nn.rgeom, st_startpoint(nn.geom)),
        nn.rules,
        nn.way_name,
        nn.geom
    FROM new_paid nn
    WHERE ST_Length(nn.geom) >= 4
"""

create_slots_for_debug = """
DROP TABLE IF EXISTS slots_debug;
CREATE TABLE slots_debug as
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
    FROM slots_likely sl
    JOIN sign s on ARRAY[s.signpost] <@ sl.signposts
    JOIN signpost_onroad spo on s.signpost = spo.id
    JOIN nextpoints np on np.slot_id = sl.id AND
                          s.signpost = np.id AND
                          s.direction = np.direction
    JOIN roads_geobase rb on spo.road_id = rb.id

    UNION ALL
    -- both direction from signpost
    SELECT
        sl.*
        , s.code
        , s.description
        , s.direction
        , spo.isleft
        , rb.name
    FROM slots_likely sl
    JOIN sign s on ARRAY[s.signpost] <@ sl.signposts and direction = 0
    JOIN signpost_onroad spo on s.signpost = spo.id
    JOIN roads_geobase rb on spo.road_id = rb.id
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
