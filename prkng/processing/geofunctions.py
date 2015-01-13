

# st_isLeft(line geometry, point geometry)

# Returns 1 if the given point is on the left side of the line geometry (given the line digit order
# Returns 0 if the given point is colinear to the line
# Returns -1 if the given point is on the right side of the line).

# examples :

# select st_isLeft('linestring(0 0, 2 0, 2 2, 5 2, 5 1, 7 1)'::geometry, 'point(1 2)'::geometry);

# select
#     label, x, y, st_isLeft('linestring(0 0, 2 0, 2 2, 5 2, 5 1, 7 1)'::geometry, st_makepoint(x,y))
# from (
#     values ('A', 1, 2), ('B', 3, 4), ('F', 4, -1), ('E', 7, -3), ('D', 8, 2), ('C', -2, 1)
# ) as pts(label, x, y)

st_isleft = """
DROP FUNCTION IF EXISTS st_isleft(geometry, geometry);
CREATE OR REPLACE FUNCTION st_isleft(line geometry, porig geometry)
  RETURNS integer AS
$BODY$
declare
    pfrom geometry;
    pto geometry;
    pct double precision;
    step double precision;
    res double precision;
begin
    pct := st_line_locate_point(line, porig);
    pfrom := st_line_interpolate_point(line, pct);
    -- case of colinearity
    if st_distance(pfrom, porig) < 0.1 then return 0 ;end if;
    -- step is percentage per meter
    step := 1 / st_length(line);

    if pct > 0.98 then
    -- if point is projected near the last point of the linestring
    pto := pfrom;
    pfrom := st_line_interpolate_point(line, GREATEST(0, pct-step));
    else
        pto := st_line_interpolate_point(line, LEAST(1, pct+step));
    end if;
    -- calculate atan2
    res := sign(atan2(
    (st_x(porig) - st_x(pfrom))*(st_y(pto) - st_y(pfrom)) - (st_x(pto) - st_x(pfrom))*(st_y(porig) - st_y(pfrom)),
    (st_x(pto) - st_x(pfrom))*(st_x(porig) - st_x(pfrom)) + (st_y(pto) - st_y(pfrom))*(st_y(porig) - st_y(pfrom))
    ));
    return -res;
end;
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 100;
"""

# converts a base 10 hour to hour:minutes
to_time_func = """
CREATE OR REPLACE FUNCTION to_time(numeric) RETURNS varchar
    AS 'SELECT trunc($1) || '':'' || trunc(mod($1, 1) *60);'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
"""
