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

    @staticmethod
    def update():
        start = datetime.datetime.now()
        finish = start - datetime.timedelta(minutes=5)

        db.engine.execute("""
            INSERT INTO free_spaces (slot_ids)
              SELECT array_agg(s.id) FROM slots s
                JOIN car2go c ON c.slot_id = s.id
                WHERE c.in_lot = false
                  AND c.parked = false
                  AND c.since  > '{}'
                  AND c.since  < '{}'
        """.format(finish.strftime('%Y-%m-%d %H:%M:%S'), start.strftime('%Y-%m-%d %H:%M:%S')))
