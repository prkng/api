from prkng.database import db, metadata

import datetime
from sqlalchemy import Column, DateTime, Integer, String, Table, text


free_spaces_table = Table(
    'free_spaces',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('time', DateTime, server_default=text('NOW()')),
    Column('address', String)
)

class FreeSpaces(object):
    @staticmethod
    def get(minutes=5):
        """
        Get slots with car2gos that have recently left
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
