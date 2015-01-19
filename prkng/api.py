# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

"""
from time import strftime
from aniso8601 import parse_datetime

from flask import render_template, Response
from flask.ext.restplus import Api, Resource, fields
from geojson import FeatureCollection, Feature

from .models import SlotsModel

GEOM_TYPES = ('Point', 'LineString', 'Polygon',
              'MultiPoint', 'MultiLineString', 'MultiPolygon')


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


# define response models

@api.model(fields={
    'type': fields.String(description='The GeoJSON Type', required=True, enum=GEOM_TYPES),
    'coordinates': fields.List(
        fields.Raw,
        description='The geometry as coordinates lists',
        required=True),
})
class Geometry(fields.Raw):
    pass


@api.model(fields={
    'description': fields.String(
        description='The description of the parking rule',
        required=True),
    'season_start': fields.String(
        description='when the permission begins in the year (ex: 12-01 for december 1)',
        required=True),
    'season_end': fields.String(
        description='when the permission no longer applies',
        required=True),
    'time_max_parking': fields.String(
        description='restriction on parking time (minutes)',
        required=True),
    'time_start': fields.Float(
        description='hour of the day when the permission starts',
        required=True),
    'time_end': fields.Float(
        description='hour of the day when the permission ends (null if beyond the day) ',
        required=True),
    'time_duration': fields.Float(
        description='permission duration',
        required=True),
    'days': fields.List(
        fields.Integer,
        description='list of days when the permission apply (1: monday, ..., 7: sunday)',
        required=True),
    'special_days': fields.String(
        description='school days for example', required=True),
    'restrict_typ': fields.String(
        description='special permissions details (may not be used for the v1 i think)',
        required=True)
})
class SlotsField(fields.Raw):
    pass

slots_fields = api.model('GeoJSONFeature', {
    'id': fields.String(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': Geometry(required=True),
    'properties': SlotsField(required=True),
})

slots_collection_fields = api.model('GeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': api.as_list(fields.Nested(slots_fields))
})


# endpoints

@api.route('/slot/<string:id>')
@api.doc(
    params={'id': 'slot id'},
    responses={404: "feature not found"}
)
class SlotResource(Resource):
    @api.marshal_list_with(slots_fields)
    def get(self, id):
        """
        Returns the parking slot corresponding to the id
        """
        res = SlotsModel.get_byid(id)
        if not res:
            api.abort(404, "feature not found")

        res = res[0]
        return Feature(
            id=res[0],
            geometry=res[1],
            properties={
                field: res[num]
                for num, field in enumerate(SlotsModel.properties[2:], start=2)
            }
        ), 200


# validate timestamp and returns it
timestamp = lambda x: parse_datetime(x).isoformat('T')


slot_parser = api.parser()
slot_parser.add_argument(
    'radius',
    type=int,
    location='args',
    default=300,
    help='Radius search in meters; default is 300'
)
slot_parser.add_argument(
    'checkin',
    type=timestamp,
    location='args',
    default=strftime("%Y-%m-%dT%H:%M:%S"),
    help="Check-in timestamp in ISO 8601 ('2013-01-01T12:00') ; default is now"
)
slot_parser.add_argument(
    'duration',
    type=float,
    location='args',
    default=1,
    help='Desired Parking time in hours ; default is 1 hour'
)


@api.route('/slots/<x>/<y>')
@api.doc(
    params={
        'x': 'Longitude location',
        'y': 'Latitude location',
    },
    responses={404: "no feature found"}
)
class SlotsResource(Resource):
    @api.marshal_list_with(slots_collection_fields)
    @api.doc(parser=slot_parser)
    def get(self, x, y):
        """
        Returns slots around the point defined by (x, y)

        """
        args = slot_parser.parse_args()
        res = SlotsModel.get_within(
            x, y,
            args['radius'],
            args['duration'],
            args['checkin']
        )

        if not res:
            api.abort(404, "no feature found")

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=feat[1],
                properties={
                    field: feat[num]
                    for num, field in enumerate(SlotsModel.properties[2:], start=2)
                }
            )
            for feat in res
        ]), 200


@api.route('/map/slots/<x>/<y>')
@api.hide
class SlotsOnMap(Resource):
    def get(self, x, y):
        """
        Backdoor to check results on a map

        """
        args = slot_parser.parse_args()
        res = SlotsModel.get_within(
            x, y,
            args['radius'],
            args['duration'],
            args['checkin']
        )

        if not res:
            api.abort(404, "no feature found")

        resp = Response(render_template('map.html', geojson=FeatureCollection([
            Feature(
                id=feat[0],
                geometry=feat[1],
                properties={
                    field: feat[num]
                    for num, field in enumerate(SlotsModel.properties[2:], start=2)
                }
            )
            for feat in res
        ])), mimetype='text/html')
        return resp
