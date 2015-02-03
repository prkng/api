# -*- coding: utf-8 -*-

from .database import db

from prkng.logger import Logger

class SlotsModel(object):
    properties = (
        'id',
        'geojson',
        'description',
        'season_start',
        'season_end',
        'time_max_parking',
        'agenda',
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
            , EXTRACT (month from ts) as month
            , (EXTRACT (isodow from ts))::varchar as dow
            , (EXTRACT (day from ts))::int as day
            , ts::date as date
        from param
        ), result as (
        select
            rank() over (partition by s.signpost order by elevation DESC) as rank
            , s.*
        from
            slots s
            , tmp t
            , jsonb_array_elements(s.agenda->t.dow) as elem -- lateral join inside !
        where
            ST_Dwithin(st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857), geom, {radius})
            AND (duration || 'hours')::interval <= coalesce((time_max_parking || 'minutes')::interval, ('365days')::interval)
            AND NOT (
                date_equality(
                    split_part(season_start, '-', 2)::int,
                    split_part(season_start, '-', 1)::int,
                    split_part(season_end, '-', 2)::int,
                    split_part(season_end, '-', 1)::int,
                    day,
                    month::int) -- test season matching
                AND tsrange(
                    date + to_time(coalesce(elem->>0, '0')::numeric)::time,
                    date + to_time(coalesce(elem->>1, '0')::numeric)::time
                ) && tsrange(ts, ts + (duration || 'hours')::interval)
            )
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
