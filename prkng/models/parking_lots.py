from prkng.database import db, metadata

from sqlalchemy import Boolean, Column, Integer, String, Table
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry


lots_table = Table(
    'parking_lots',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('active', Boolean, default=True),
    Column('partner_id', String, nullable=True),
    Column('partner_name', String, nullable=True),
    Column('city', String),
    Column('name', String),
    Column('operator', String, nullable=True),
    Column('capacity', Integer, nullable=True),
    Column('available', Integer, nullable=True),
    Column('address', String),
    Column('description', String),
    Column('geom', Geometry('POINT', 3857)),
    Column('geojson', JSONB),
    Column('agenda', JSONB),
    Column('attrs', JSONB),
    Column('street_view', JSONB)
)


class ParkingLots(object):
    properties = (
        'id',
        'geojson',
        'city',
        'name',
        'partner_name',
        'partner_id',
        'operator',
        'capacity',
        'available',
        'address',
        'agenda',
        'attrs',
        'street_view'
    )

    @staticmethod
    def get_all():
        """
        Retrieve the nearest parking lots/garages within ``radius`` meters of a
        given location (x, y).
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
        """.format(properties=','.join(ParkingLots.properties))

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_within(x, y, radius):
        """
        Retrieve the nearest parking lots/garages within ``radius`` meters of a
        given location (x, y).
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE
            active = true
            AND ST_Dwithin(
                st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                geom,
                {radius}
            )
        """.format(
            properties=','.join(ParkingLots.properties),
            x=x,
            y=y,
            radius=radius
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_nearest(x, y, limit):
        """
        Retrieve the nearest X parking lots/garages to a given location (x, y).
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
        ORDER BY ST_Distance(geom, st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857))
        LIMIT {limit}
        """.format(
            properties=','.join(ParkingLots.properties),
            x=x,
            y=y,
            limit=limit
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_boundbox(nelat, nelng, swlat, swlng):
        """
        Retrieve all parking lots / garages inside a given boundbox.
        """
        req = """
        SELECT {properties} FROM parking_lots
        WHERE active = true
            AND ST_intersects(
                ST_Transform(
                    ST_MakeEnvelope({nelng}, {nelat}, {swlng}, {swlat}, 4326),
                    3857
                ),
                parking_lots.geom
            )
        """.format(
            properties=','.join(ParkingLots.properties),
            nelat=nelat,
            nelng=nelng,
            swlat=swlat,
            swlng=swlng
        )

        return db.engine.execute(req).fetchall()

    @staticmethod
    def get_byid(lid):
        """
        Retrieve lot/garage information by its ID
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM parking_lots
            WHERE id = {sid}
            """.format(sid=lid, properties=','.join(ParkingLots.properties))).fetchall()

    @staticmethod
    def get_bypartnerid(pname, pid):
        """
        Retrieve lot/garage information by its partner ID
        """
        return db.engine.execute("""
            SELECT {properties}
            FROM parking_lots
            WHERE partner_name = '{pname}' AND partner_id = '{pid}'
            """.format(pname=pname, pid=pid, properties=','.join(ParkingLots.properties))).fetchall()
