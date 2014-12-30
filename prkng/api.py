# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

"""
from flask.ext.restplus import Api, Resource

from database import db

# api instance
api = Api(
    version='1.0',
    title='Prkng API',
    description='An API to access free parking slots in some cities of Canada',
)


def init_api(app):
    """
    Initialize API into flask application
    """
    api.init_app(app)


@api.route('/coverage')
class coverage(Resource):
    def get(self):
        """
        Returns the list of available cities
        """
        db.connection.query("select 1")
        return ['Montreal', 'Quebec']
