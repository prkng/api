from prkng.database import db, metadata
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Table, text


checkin_table = Table(
    'checkins',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('slot_id', Integer),
    Column('long', Float),
    Column('lat', Float),
    Column('checkin_time', DateTime, server_default=text('NOW()'), index=True),
    Column('checkout_time', DateTime)
)

class Checkins(object):
    @staticmethod
    def get(user_id):
        """
        Get info on the user's current check-in
        """
        res = db.engine.execute("""
            SELECT c.id, c.slot_id, s.way_name, c.long, c.lat,
                to_char(c.checkin_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkin_time,
                to_char(c.checkout_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkout_time,
                c.checkout_time IS NULL AS active
            FROM checkins c
            JOIN slots s ON c.slot_id = s.id
            WHERE c.user_id = {uid}
                AND c.checkout_time IS NULL
            ORDER BY c.checkin_time DESC
            LIMIT 1
        """.format(uid=user_id)).first()
        if not res:
            return None
        return dict(res)

    @staticmethod
    def get_all(user_id, limit):
        res = db.engine.execute("""
            SELECT c.id, c.slot_id, s.way_name, c.long, c.lat,
                to_char(c.checkin_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkin_time,
                to_char(c.checkout_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkout_time,
                c.checkout_time IS NULL AS active
            FROM checkins c
            JOIN slots s ON c.slot_id = s.id
            WHERE c.user_id = {uid}
            ORDER BY c.checkin_time DESC
            LIMIT {limit}
            """.format(uid=user_id, limit=limit)).fetchall()
        return [dict(row) for row in res]

    @staticmethod
    def add(user_id, slot_id):
        exists = db.engine.execute("""
            SELECT 1 FROM slots WHERE id = {slot_id}
        """.format(slot_id=slot_id)).first()
        if not exists:
            return False

        # if the user is already checked in elsewhere, deactivate their old checkin
        db.engine.execute(checkin_table.update().where((checkin_table.c.user_id == user_id) & \
            (checkin_table.c.checkout_time == None)).values(checkout_time=text('NOW()')))

        res = db.engine.execute("""
            INSERT INTO checkins (user_id, slot_id, long, lat)
            SELECT
                {user_id}, {slot_id},
                (button_location->>'long')::float,
                (button_location->>'lat')::float
            FROM slots WHERE id = {slot_id}
            RETURNING *
        """.format(user_id=user_id, slot_id=slot_id)).first()
        res = dict(res)
        res["checkin_time"] = res["checkin_time"].isoformat()
        res["checkout_time"] = res["checkout_time"].isoformat() if res["checkout_time"] else None
        return res

    @staticmethod
    def delete(user_id, checkin_id):
        db.engine.execute(checkin_table.update().where((checkin_table.c.user_id == user_id) & \
            (checkin_table.c.id == cid)).values(checkout_time=text('NOW()'))
        return True
