from prkng.database import db, metadata

import datetime
from sqlalchemy import Column, DateTime, Integer, String, Table, text
from sqlalchemy.dialects.postgresql import ARRAY


free_spaces_table = Table(
    'free_spaces',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('time', DateTime, server_default=text('NOW()')),
    Column('address', String),
    Column('slot_ids', ARRAY(Integer))
)

class FreeSpaces(object):
    """
    An object to manage free space data.

    A 'free space' is created when a carshare has been recorded as leaving a slot on the street. It can be assumed that the departure of the carshare has created a free space to park on-street, which would be of use to users trying to park in that neighbourhood.
    """

    @staticmethod
    def get(minutes=5):
        """
        Get free spaces with carshares that have recently left.
        A free space object consists of the slot ID, the way name, its geometry object, applicable rules for that slot, and the approximate time of departure.

        :param minutes: Max age of the carshare departure. Default 5 (int)
        :returns: Free Space object (dict)
        """
        res = db.engine.execute("""
            SELECT
                s.id,
                s.way_name,
                s.geojson,
                s.rules,
                s.button_location->>'lat' AS lat,
                s.button_location->>'long' AS long,
                to_char(f.time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS since
            FROM slots s
            JOIN free_spaces f ON f.time >= (NOW() - INTERVAL '{} MIN')
                AND s.id = ANY(f.slot_ids)
        """.format(minutes))
        return [
            {key: value for key, value in row.items()}
            for row in res
        ]
