# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

"""
from flask.ext.restplus import Api, Resource
from geojson import FeatureCollection, loads, Feature

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


@api.route('/cities')
class cities(Resource):
    def get(self):
        """
        Returns the list of available cities
        """
        return ['Montreal', 'Quebec']


@api.route('/slot/<string:id>')
@api.doc(params={'id': 'slot id'},
         responses={404: "feature not found"})
class slot(Resource):
    def get(self, id):
        """
        Returns the parking slot corresponding to the id
        """
        properties = (
            'osm_id',
            'code',
            'description',
            'season_start',
            'season_end',
            'time_max_parking',
            'time_start',
            'time_end',
            'time_duration',
            'lun',
            'mar',
            'mer',
            'jeu',
            'ven',
            'sam',
            'dim',
            'daily',
            'special_days',
            'restrict_typ',
        )
        res = db.connection.query("""SELECT
            id
            , ST_AsGeoJSON(geom)
            , {prop}
        FROM slots
        WHERE id = {id}""".format(id=id, prop=','.join(properties)))

        if not res:
            api.abort(404, "feature not found")

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=loads(feat[1]),
                properties={
                    field: feat[num]
                    for num, field in enumerate(properties, start=2)
                }
            )
            for feat in res
        ]), 200


@api.route('/slots/<string:x>/<string:y>/<string:radius>')
@api.doc(
    params={
        'x': 'x coordinate',
        'y': 'y coordinate',
        'radius': 'radius',
    },
    responses={404: "no feature found"})
class slots(Resource):
    def get(self, x, y, radius):
        """
        Returns a list of slots around the point defined by (x, y)
        """
        api.abort(404, "not yet implemented")
