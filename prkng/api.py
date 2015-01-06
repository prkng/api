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

SLOT_PROPERTIES = (
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


@api.route('/slot/<string:id>')
@api.doc(params={'id': 'slot id'},
         responses={404: "feature not found"})
class slot(Resource):
    def get(self, id):
        """
        Returns the parking slot corresponding to the id
        """
        res = db.connection.query("""SELECT
            id
            , ST_AsGeoJSON(st_transform(geom, 4326))
            , {prop}
        FROM slots
        WHERE id = {id}""".format(id=id, prop=','.join(SLOT_PROPERTIES)))

        if not res:
            api.abort(404, "feature not found")

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=loads(feat[1]),
                properties={
                    field: feat[num]
                    for num, field in enumerate(SLOT_PROPERTIES, start=2)
                }
            )
            for feat in res
        ]), 200


@api.route('/slots/<string:x>/<string:y>/<string:radius>')
@api.doc(
    params={
        'x': 'x coordinate (longitude in wgs84)',
        'y': 'y coordinate (latitude in wgs84)',
        'radius': 'radius',
    },
    responses={404: "no feature found"})
class slots(Resource):
    def get(self, x, y, radius):
        """
        Returns a list of slots around the point defined by (x, y)
        Example : -73.5830569267273, 45.55033143523324
        """
        res = db.connection.query("""SELECT
            id
            , ST_AsGeoJSON(st_transform(geom, 4326))
            , {prop}
        FROM slots
        WHERE ST_Dwithin(
            st_transform('SRID=4326;POINT({x} {y})'::geometry, 3857),
            geom,
            {radius}
        )
        """.format(prop=','.join(SLOT_PROPERTIES), x=x, y=y, radius=radius))

        if not res:
            api.abort(404, "no feature found")

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=loads(feat[1]),
                properties={
                    field: feat[num]
                    for num, field in enumerate(SLOT_PROPERTIES, start=2)
                }
            )
            for feat in res
        ]), 200
