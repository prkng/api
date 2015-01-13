# -*- coding: utf-8 -*-

from .database import db


class SlotsModel(object):
    properties = (
        'id',
        'geojson',
        'description',
        'season_start',
        'season_end',
        'time_max_parking',
        'time_start',
        'time_end',
        'time_duration',
        'days',
        'special_days',
        'restrict_typ'
    )

    @staticmethod
    def get_within(x, y, radius, duration, checkin):
        """
        Retrieve the nearest slots within ``radius`` meters of
        given location (x, y)
        """
        checkin = "'%s'::timestamp" % checkin if checkin else 'LOCALTIMESTAMP'

        req = """
        with param as (
            select {checkin} as ts,
            {duration} as duration
        ), tmp as (
        select
            ts
            , duration
            , EXTRACT (year from ts) as year
            , EXTRACT (isodow from ts) as dow
            , ts::date as date
        from param
        ), result as (
        select
            rank() over (partition by s.signpost order by elevation DESC) as rank
            , s.*
        from slots s, tmp t
        where
            ST_Dwithin(
                st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                geom,
                {radius})

            AND ARRAY[t.dow::integer] <@ s.days
            AND duration <= coalesce(s.time_max_parking / 60, time_duration)
            AND restrict_typ is NULL
            -- check season connection
            AND tsrange(
                coalesce((year || '-' || season_start)::timestamp, (year || '-01-01')::timestamp),
                coalesce((year || '-' || season_end)::timestamp, (year || '-12-31T23:59:59')::timestamp),
                '[]' -- bounds included
              ) @> ts
            -- check hours coverage
            AND tsrange(
                date + to_time(s.time_start::numeric)::time,
                coalesce(date + to_time(s.time_end::numeric)::time,
                         date + to_time(s.time_start::numeric)::time + (time_duration || 'hours')::interval),
                '[]' -- bounds included
              ) @> tsrange(ts, ts + (duration || 'hours')::interval)
        )
            SELECT {properties}
            FROM result WHERE rank = 1
        """.format(
            properties=','.join(SlotsModel.properties),
            x=x,
            y=y,
            radius=radius,
            checkin=checkin,
            duration=duration
        )
        return db.connection.query(req)

    @staticmethod
    def get_byid(sid):
        """
        Retrieve the nearest slots within ``radius`` meters of
        given location (x, y)
        """
        return db.connection.query("""
            SELECT {properties}
            FROM slots
            WHERE id = {sid}
            """.format(sid=sid, properties=','.join(SlotsModel.properties)))
