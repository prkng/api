from prkng.database import db, metadata

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, Table, text


car2go_table = Table(
    'car2go',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('slot_id', Integer),
    Column('vin', String, unique=True),
    Column('name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('address', String),
    Column('since', DateTime, server_default=text('NOW()')),
    Column('parked', Boolean),
    Column('in_lot', Boolean)
)


class Car2Go(object):
    @staticmethod
    def get(name):
        """
        Get a car2go by its name.
        """
        res = db.engine.execute("SELECT * FROM car2go WHERE name = {}".format(name)).first()
        return {key: value for key, value in res.items()}

    @staticmethod
    def get_all(city):
        """
        Get all active car2go records.
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
            FROM car2go c
            JOIN slots s ON c.city = s.city AND c.slot_id = s.id
            WHERE c.city = '{city}'
                AND c.in_lot = false
                AND c.parked = true
        """.format(city=city)).fetchall()
        return [
            {key: value for key, value in row.items()}
            for row in res
        ]
