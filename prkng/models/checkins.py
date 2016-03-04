from prkng.database import db, metadata
from prkng.models.analytics import event_table
from sqlalchemy import desc, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, Table, text


checkin_table = Table(
    'checkins',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('city', String),
    Column('slot_id', Integer),
    Column('long', Float),
    Column('lat', Float),
    Column('checkin_time', DateTime, server_default=text('NOW()'), index=True),
    Column('checkout_time', DateTime),
    Column('active', Boolean, default=True),
    Column('is_hidden', Boolean, default=False)
)

class Checkins(object):
    """
    A class to manage user-generated Checkins.

    A checkin represents a saved position of the user's location, parking 'slot', and checkin/checkout time.
    The checkin is used to manage a wide variety of analytics and interactions between the app and the user.
    """

    @staticmethod
    def get(user_id):
        """
        Get info on the user's current (or most recent) check-in.

        :param user_id: user ID (int)
        :returns: Checkin object (dict)
        """
        res = db.engine.execute("""
            SELECT c.id, c.city, c.slot_id, s.way_name, c.long, c.lat, c.active,
                to_char(c.checkin_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkin_time,
                to_char(c.checkout_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkout_time
            FROM checkins c
            JOIN slots s ON c.city = s.city AND c.slot_id = s.id
            WHERE c.user_id = {uid}
                AND c.active = true
            ORDER BY c.checkin_time DESC
            LIMIT 1
        """.format(uid=user_id)).first()
        if not res:
            return None
        return dict(res)

    @staticmethod
    def get_byid(id):
        """
        Get info on a checkin by its ID.

        :param id: checkin ID (int)
        :returns: Checkin object (dict)
        """
        res = db.engine.execute("""
            SELECT c.id, c.city, c.slot_id, s.way_name, c.long, c.lat, c.active,
                to_char(c.checkin_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkin_time,
                to_char(c.checkout_time, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS checkout_time
            FROM checkins c
            JOIN slots s ON c.city = s.city AND c.slot_id = s.id
            WHERE c.id = {id}
        """.format(id=id)).first()
        if not res:
            return None
        return dict(res)

    @staticmethod
    def get_all(user_id, limit):
        """
        Get most recent checkins for a user.

        :param user_id: user ID (int)
        :param limit: number of checkins to return (int)
        :returns: list of Checkin objects (dicts)
        """
        res = db.engine.execute("""
            SELECT c.id, c.city, c.slot_id, s.way_name, c.long, c.lat, c.active,
                c.checkin_time, c.checkout_time
            FROM checkins c
            JOIN slots s ON c.city = s.city AND c.slot_id = s.id
            WHERE c.user_id = {uid} AND c.is_hidden != true
            ORDER BY c.checkin_time DESC
            LIMIT {limit}
            """.format(uid=user_id, limit=limit)).fetchall()
        return [dict(row) for row in res]

    @staticmethod
    def add(user_id, slot_id):
        """
        Add a new checkin.
        Automatically saves city, location and time.

        :param user_id: user ID (int)
        :param slot_id: slot to check into (int)
        :returns: Checkin object (dict)
        """
        # if the user is already checked in elsewhere, deactivate their old checkin
        db.engine.execute(checkin_table.update().where((checkin_table.c.user_id == user_id) & \
            (checkin_table.c.checkout_time == None)).values(active=False))

        cid = db.engine.execute("""
            INSERT INTO checkins (user_id, city, slot_id, long, lat, active)
            SELECT
                {user_id}, city, {slot_id},
                ST_X(ST_Line_Interpolate_Point(ST_Transform(geom, 4326), 0.5)),
                ST_Y(ST_Line_Interpolate_Point(ST_Transform(geom, 4326), 0.5)),
                true
            FROM slots WHERE id = {slot_id}
            RETURNING id
        """.format(user_id=user_id, slot_id=slot_id)).first()
        return Checkins.get_byid(cid[0])

    @staticmethod
    def remove(user_id, checkin_id, left=True):
        """
        Checkout.

        :param user_id: user ID (int)
        :param checkin_id: checkin ID (int)
        :param left: True if the last fence departure should be used as checkout time (bool)
        :returns: True
        """
        # get last fence departure time, use as checkout time if user has left
        res = event_table.select((event_table.c.user_id == user_id) &\
                (event_table.c.event == 'left_fence')).order_by(desc(event_table.c.created))\
                .execute().first()

        db.engine.execute(checkin_table.update().where((checkin_table.c.user_id == user_id) & \
            (checkin_table.c.id == checkin_id)).values(active=False,
            checkout_time=res["created"] if res and left else None))
        return True

    @staticmethod
    def update(user_id, checkin_id, is_hidden):
        """
        Update a checkin. Used to flag the checkin as hidden if the user deletes it in their app history.

        :param user_id: user ID (int)
        :param checkin_id: checkin ID (int)
        :param is_hidden: True if the checkin should be hidden (bool)
        """
        cid = db.engine.execute("""
            UPDATE checkins SET is_hidden = {ishidden}
            WHERE user_id = {user_id} AND id = {checkin_id}
        """.format(ishidden=str(is_hidden).lower(), user_id=user_id,
            checkin_id=checkin_id))

    @staticmethod
    def clear_history(user_id):
        """
        Flag all a user's checkins to no longer display for history (`get_all`) calls.

        :param user_id: user ID (int)
        """
        db.engine.execute(checkin_table.update().where(checkin_table.c.user_id == user_id)\
            .values(is_hidden=True))
