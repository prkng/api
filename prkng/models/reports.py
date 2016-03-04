from prkng.database import db, metadata
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Table, text
from sqlalchemy.dialects.postgresql import ARRAY


report_table = Table(
    'reports',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('city', String),
    Column('signposts', ARRAY(Integer)),
    Column('way_name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('image_url', String),
    Column('notes', String),
    Column('progress', Integer, server_default="0")
)


class Reports(object):
    """
    A class to manage and retrieve user-generated Reports.

    Reports are submissions made to Prkng by users that wish to inform us of incorrect data they found for parking spaces on-street. We use this information to create Corrections that will correct the incorrect data until the city does so themselves. Reports include user data, location data, notes and a photo of the correct regulation.
    """

    @staticmethod
    def add(user_id, city, slot_id, lng, lat, url, notes):
        """
        Add a new report.

        :param user_id: user ID (int)
        :param city: city name (str)
        :param slot_id: slot ID (opt int)
        :param lng: longitude (int)
        :param lat: latitude (int)
        :param url: URL for the report image (str)
        :param notes: user-generated notes for this report (str/unicode)
        """
        db.engine.execute("""
            INSERT INTO reports (user_id, city, signposts, way_name, long, lat, image_url, notes)
            SELECT {user_id}, '{city}', s.signposts, s.way_name, {lng}, {lat},
                '{image_url}', '{notes}'
              FROM slots s
              WHERE s.city = '{city}'
                AND s.id = {slot_id}
        """.format(user_id=user_id, city=city, slot_id=slot_id or "NULL", lng=lng, lat=lat,
            image_url=url, notes=notes.encode("utf-8").replace("'", "''")))

    @staticmethod
    def get(id):
        """
        Get a report based on its ID.

        :param id: report ID (int)
        :returns: Report object (dict)
        """
        res = db.engine.execute("""
            SELECT
                r.id,
                to_char(r.created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created,
                r.city,
                s.id AS slot_id,
                u.id AS user_id,
                u.name AS user_name,
                u.email AS user_email,
                s.way_name,
                s.rules,
                r.long,
                r.lat,
                r.image_url,
                r.notes,
                r.progress,
                ARRAY_REMOVE(ARRAY_AGG(c.id), NULL) AS corrections
            FROM reports r
            JOIN users u ON r.user_id = u.id
            LEFT JOIN slots s ON r.city = s.city AND r.signposts = s.signposts
            LEFT JOIN corrections c ON s.signposts = c.signposts
            WHERE r.id = {}
            GROUP BY r.id, u.id, s.way_name, s.rules
        """.format(id)).first()

        return {key: value for key, value in res.items()}

    @staticmethod
    def set_progress(id, progress):
        """
        Set the internal processing progress of this report.
        Used in Admin interface.

        :param id: report ID (int)
        :param progress: progress value (int)
        :returns: Report object (dict)
        """
        res = db.engine.execute("""
            UPDATE reports r
              SET progress = {}
              WHERE r.id = {}
        """.format(progress, id))

        return Reports.get(id)

    @staticmethod
    def delete(id):
        """
        Delete a report.

        :param id: report ID (int)
        """
        db.engine.execute(report_table.delete().where(report_table.c.id == id))
