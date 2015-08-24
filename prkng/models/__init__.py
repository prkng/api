from assets import Images
from car2gos import Car2Go
from checkins import Checkins
from cities import City
from corrections import Corrections
from free_spaces import FreeSpaces
from garages import Garages
from reports import Reports
from slots import Slots
from users import User, UserAuth

from prkng.database import db, metadata
from sqlalchemy import create_engine


def init_model(app):
    """
    Initialize DB engine and create tables
    """
    if app.config['TESTING']:
        DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=app.config['PG_TEST_USERNAME'],
            password=app.config['PG_TEST_PASSWORD'],
            host=app.config['PG_TEST_HOST'],
            port=app.config['PG_TEST_PORT'],
            database=app.config['PG_TEST_DATABASE'],
        )
    else:
        DATABASE_URI = 'postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=app.config['PG_USERNAME'],
            password=app.config['PG_PASSWORD'],
            host=app.config['PG_HOST'],
            port=app.config['PG_PORT'],
            database=app.config['PG_DATABASE'],
        )

    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI

    # lazy bind the sqlalchemy engine
    with app.app_context():
        db.engine = create_engine(
            '{SQLALCHEMY_DATABASE_URI}'.format(**app.config),
            strategy='threadlocal',
            pool_size=10
        )

    metadata.bind = db.engine
    # create model
    metadata.create_all()
