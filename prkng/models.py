# -*- coding: utf-8 -*-
from .database import db
from .filters import on_restriction

class SlotsModel(object):
    properties = (
        'id',
        'geojson',
        'rules'
    )

    @staticmethod
    def get_within(x, y, radius, duration, checkin):
        """
        Retrieve the nearest slots within ``radius`` meters of
        given location (x, y).

        Restriction reduction before output
        """
        checkin = checkin or datetime.now()

        req = """
        SELECT {properties}
        FROM slots
        WHERE
            ST_Dwithin(st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857), geom, {radius})
        """.format(
            properties=','.join(SlotsModel.properties),
            x=x,
            y=y,
            radius=radius
        )

        features = db.connection.query(req)
        return filter(
            lambda x: not on_restriction(x[2], checkin, duration),
            features
        )

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





