from prkng.database import db, metadata

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Table, text
from sqlalchemy.dialects.postgresql import JSONB


carshares_table = Table(
    'carshares',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('slot_id', Integer),
    Column('company', String),
    Column('vin', String, unique=True, nullable=True),
    Column('name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('address', String),
    Column('fuel', Integer),
    Column('since', DateTime, server_default=text('NOW()')),
    Column('parked', Boolean),
    Column('in_lot', Boolean),
    Column('geojson', JSONB)
)


class Carshares(object):
    properties = (
        'id',
        'geojson',
        'company',
        'name',
        'fuel'
    )

    @staticmethod
    def get(company, name):
        """
        Get a carshare by its company and car name.
        """
        res = db.engine.execute("""
            SELECT * FROM carshares WHERE company = '{}' AND name = '{}' LIMIT 1
        """.format(company, name)).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def get_within(city, x, y, radius, company=False):
        """
        Get all carshares in a city within a particular radius.
        """
        res = db.engine.execute("""
            SELECT {properties} FROM carshares c
            WHERE c.city = '{city}' AND
                ST_Dwithin(
                    st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
                    geom,
                    {radius}
                )
        """.format(properties=', '.join(Carshares.properties), city=city, x=x, y=y, radius=radius)\
        + ("AND c.company = '{co}'".format(co=company) if company else ""))
        return [{key: value for key, value in row.items()} for row in res]

    @staticmethod
    def get_all(company, city):
        """
        Get all active carshare records for a city and company.
        """
        res = db.engine.execute("""
            SELECT
                c.id,
                c.city,
                c.name,
                c.vin,
                c.address,
                to_char(c.since, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS since,
                c.long,
                c.lat,
                s.rules
            FROM carshares c
            JOIN slots s ON c.city = s.city AND c.slot_id = s.id
            WHERE  c.company = '{company}'
                AND c.city   = '{city}'
                AND c.in_lot = false
                AND c.parked = true
        """.format(company=company, city=city)).fetchall()
        return [
            {key: value for key, value in row.items()}
            for row in res
        ]
