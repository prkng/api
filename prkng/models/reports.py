from prkng.database import db, metadata
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Table, text


report_table = Table(
    'reports',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('slot_id', Integer),
    Column('way_name', String),
    Column('long', Float),
    Column('lat', Float),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('image_url', String),
    Column('notes', String),
    Column('progress', Integer, server_default="0")
)


class Reports(object):
    @staticmethod
    def add(user_id, slot_id, lng, lat, url, notes):
        db.engine.execute("""
            INSERT INTO reports (user_id, slot_id, way_name, long, lat, image_url, notes)
            SELECT {user_id}, s.signposts, s.way_name, {lng}, {lat}, '{image_url}', '{notes}'
              FROM slots s
              WHERE s.id = {slot_id}
        """.format(user_id=user_id, slot_id=slot_id or "NULL", lng=lng, lat=lat,
            image_url=url, notes=notes))

    @staticmethod
    def get(id):
        res = db.engine.execute("""
            SELECT
                r.id,
                to_char(r.created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created,
                r.slot_id,
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
            LEFT JOIN slots s ON r.signposts = s.signposts
            LEFT JOIN corrections c ON s.signposts = c.signposts
            WHERE r.id = {}
            GROUP BY r.id, u.id, s.way_name, s.rules
        """.format(id)).first()

        return {key: value for key, value in res.items()}

    @staticmethod
    def set_progress(id, progress):
        res = db.engine.execute("""
            UPDATE reports r
              SET progress = {}
              WHERE r.id = {}
        """.format(progress, id))

        return Reports.get(id)

    @staticmethod
    def delete(id):
        db.engine.execute(report_table.delete().where(report_table.c.id == id))
