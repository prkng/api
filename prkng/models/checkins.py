from prkng.database import db, metadata
from sqlalchemy import Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Table, text


checkin_table = Table(
    'checkins',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('slot_id', Integer),
    Column('way_name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    # The time the check-in was created.
    Column('active', Boolean)
)

class Checkins(object):
    @staticmethod
    def get(user_id):
        """
        Get info on the user's current check-in
        """
        res = db.engine.execute("""
            SELECT id, slot_id, way_name, long, lat, created::text as created, active
            FROM checkins
            WHERE user_id = {}
            AND active = true
        """.format(user_id)).first()
        if not res:
            return None
        return dict(res)

    @staticmethod
    def get_all(user_id, limit):
        res = db.engine.execute("""
            SELECT id, slot_id, way_name, long, lat, created::text as created, active
            FROM checkins
            WHERE user_id = {uid}
            ORDER BY created DESC
            LIMIT {limit}
            """.format(uid=user_id, limit=limit)).fetchall()
        return [dict(row) for row in res]

    @staticmethod
    def add(user_id, slot_id):
        exists = db.engine.execute("""
            select 1 from slots where id = {slot_id}
            """.format(slot_id=slot_id)).first()
        if not exists:
            return False

        # if the user is already checked in elsewhere, deactivate their old checkin
        db.engine.execute(checkin_table.update().where(checkin_table.c.user_id == user_id).values(active=False))

        db.engine.execute("""
            INSERT INTO checkins (user_id, slot_id, way_name, long, lat, active)
            SELECT
                {user_id}, {slot_id}, way_name,
                (button_location->>'long')::float,
                (button_location->>'lat')::float,
                true
            FROM slots WHERE id = {slot_id}
        """.format(user_id=user_id, slot_id=slot_id))  # FIXME way_name
        return True

    @staticmethod
    def delete(user_id, checkin_id):
        db.engine.execute("""
            UPDATE checkins
            SET active = false
            WHERE user_id = {}
            AND id = {}
        """.format(user_id, checkin_id))
        return True
