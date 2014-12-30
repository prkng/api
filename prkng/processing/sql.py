# force reorientation of ways from left to right
# since we don't need way direction
REORDER_LINES = """
WITH tmp AS (
SELECT
    osm_id
    ,name
    ,aerialway
    ,CASE
       when st_x(st_startpoint(way)) > st_x(st_endpoint(way))
       then st_reverse(way)
       else way
    END AS geom
FROM planet_osm_line WHERE osm_id in( 103867622, 25401652)
) SELECT
    osm_id,
    name,
    degrees(ST_Azimuth(st_startpoint(geom), st_endpoint(geom)))
FROM tmp
"""


FIND_POINT = """
select st_closestpoint(o.way, q.geom) as geom from planet_osm_line o
join quebec_panneau q on st_dwithin(q.geom, st_transform(o.way, 3857), 50)
where q.gid = 13908 and "addr:interpolation" is NULL
"""
