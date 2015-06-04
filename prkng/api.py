# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from __future__ import unicode_literals
from copy import deepcopy
from functools import wraps
from time import strftime
from aniso8601 import parse_datetime

from flask import render_template, Response, g, request
from flask.ext.restplus import Api, Resource, fields
from geojson import FeatureCollection, Feature

from .models import SlotsModel, User, Checkins, Images, Reports
from .login import facebook_signin, google_signin, email_register, email_signin, email_update

GEOM_TYPES = ('Point', 'LineString', 'Polygon',
              'MultiPoint', 'MultiLineString', 'MultiPolygon')

HEADER_API_KEY = 'X-API-KEY'


# helper to validate timestamp and returns it
def timestamp(x):
    return parse_datetime(x).isoformat(str('T'))


class PrkngApi(Api):
    """
    Subclass Api and adds a ``secure`` decorator
    """
    def __init__(self, **kwargs):
        super(PrkngApi, self).__init__(**kwargs)

    def secure(self, func):
        '''Enforce authentication'''
        @wraps(func)
        def wrapper(*args, **kwargs):

            apikey = request.headers.get(HEADER_API_KEY)

            if not apikey:
                return 'Invalid API Key', 401

            g.user = User.get_byapikey(apikey)
            if not g.user:
                return 'Invalid API Key', 401

            return func(*args, **kwargs)

        return wrapper


# api instance (Blueprint)
api = PrkngApi(
    version='1.0',
    title='Prkng API',
    description='An API to access free parking slots in some cities of Canada',
)


def init_api(app):
    """
    Initialize extensions into flask application
    """
    api.init_app(app)


# define response models
@api.model(fields={
    'type': fields.String(description='GeoJSON Type', required=True, enum=GEOM_TYPES),
    'coordinates': fields.List(
        fields.Raw,
        description='The geometry as coordinates lists',
        required=True),
})
class Geometry(fields.Raw):
    pass


@api.model(fields={
    '%s' % day: fields.List(fields.Raw)
    for day in range(1, 8)
})
class AgendaView(fields.Raw):
    pass


@api.model(fields={
    'long': fields.Float(),
    'lat': fields.Float()
})
class ButtonLocation(fields.Raw):
    pass


@api.model(fields={
    'description': fields.String(
        description='description of the parking rule',
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
    'agenda': AgendaView(
        description='''list of days when the restriction apply (1: monday, ..., 7: sunday)
                       containing a list of time ranges when the restriction apply''',
        required=True),
    'special_days': fields.String(required=True),
    'restrict_typ': fields.String(
        description='special restriction details',
        required=True),
    'button_location': ButtonLocation(required=True)
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


@api.route('/slot/<string:id>')
class SlotResource(Resource):
    @api.marshal_list_with(slots_fields)
    @api.doc(
        params={'id': 'slot id'},
        responses={404: "feature not found"}
    )
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


slot_parser = api.parser()
slot_parser.add_argument(
    'radius',
    type=int,
    location='args',
    default=300,
    help='Radius search in meters; default is 300'
)
slot_parser.add_argument(
    'latitude',
    type=float,
    location='args',
    required=True,
    help='Latitude in degrees (WGS84)'
)
slot_parser.add_argument(
    'longitude',
    type=float,
    location='args',
    required=True,
    help='Longitude in degrees (WGS84)'
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


@api.route('/slots')
class SlotsResource(Resource):
    @api.marshal_list_with(slots_collection_fields)
    @api.doc(
        responses={404: "no feature found"}
    )
    @api.doc(parser=slot_parser)
    def get(self):
        """
        Returns slots around the point defined by (x, y)
        """
        args = slot_parser.parse_args()

        res = SlotsModel.get_within(
            args['longitude'],
            args['latitude'],
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


@api.route('/map/slots')
@api.hide
class SlotsOnMap(Resource):
    def get(self):
        """
        Backdoor to view results on a map
        """
        args = slot_parser.parse_args()
        res = SlotsModel.get_within(
            args['longitude'],
            args['latitude'],
            args['radius'],
            args['duration'],
            args['checkin']
        )

        if not res:
            api.abort(404, "no feature found")

        # remove agenda since it's too big for leaflet popup
        for re in res:
            for rule in re[2]:
                for key, value in rule.items():
                    rule.pop('agenda', None)

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


token_parser = api.parser()
token_parser.add_argument(
    'access_token',
    type=str,
    location='form',
    help='Oauth2 user access token'
)

user_model = api.model('User', {
    'name': fields.String(),
    'email': fields.String(),
    'apikey': fields.String(),
    'created': fields.String(),
    'auth_id': fields.String(),
    'id': fields.String(),
    'gender': fields.String(),
    'picture': fields.String()
})


@api.route('/login/facebook')
class LoginFacebook(Resource):
    @api.doc(parser=token_parser, model=user_model)
    def post(self):
        """
        Login with a facebook account.

        Existing user will automatically have a new API key generated
        """
        args = token_parser.parse_args()

        return facebook_signin(args['access_token'])


@api.route('/login/google')
class LoginGoogle(Resource):
    @api.doc(parser=token_parser, model=user_model)
    def post(self):
        """
        Login with a google account.fields

        Existing user will automatically have a new API key generated
        """
        args = token_parser.parse_args()

        return google_signin(args['access_token'])


register_parser = api.parser()
register_parser.add_argument('email', required=True, type=str, location='form', help='user email')
register_parser.add_argument('password', required=True, type=str, location='form', help='user password')
register_parser.add_argument('name', required=True, type=unicode, location='form', help='user name')
register_parser.add_argument('gender', required=True, type=str, location='form', help='gender')
register_parser.add_argument('birthyear', required=True, type=int, location='form', help='birth year')
register_parser.add_argument('picture', type=str, location='form', help='profile picture URL')


@api.route('/register')
class Register(Resource):
    @api.doc(parser=register_parser, model=user_model)
    def post(self):
        """
        Register a new account.
        """
        args = register_parser.parse_args()
        return email_register(**args)


email_parser = api.parser()
email_parser.add_argument('email', type=str, location='form', help='user email')
email_parser.add_argument('password', type=str, location='form', help='user password')


@api.route('/login/email')
class LoginEmail(Resource):
    @api.doc(parser=email_parser, model=user_model)
    def post(self):
        """
        Login with en email account.
        """
        args = email_parser.parse_args()
        return email_signin(**args)


# define header parser for the API key
api_key_parser = api.parser()
api_key_parser.add_argument(
    'X-API-KEY',
    type=str,
    location='headers',
    help='Prkng API Key'
)

# define the slot id parser
post_checkin_parser = deepcopy(api_key_parser)
post_checkin_parser.add_argument(
    'slot_id', type=int, required=True, help='Slot identifier', location='form')

get_checkin_parser = deepcopy(api_key_parser)
get_checkin_parser.add_argument(
    'limit', type=int, default=10, help='Slot identifier', location='query')


@api.route('/slot/checkin')
class Checkin(Resource):
    @api.doc(parser=get_checkin_parser,
             responses={401: "Invalid API key"})
    @api.secure
    def get(self):
        """
        Get the list of last checkins.

        List has a max length of 10 checkins.
        """
        args = get_checkin_parser.parse_args()
        limit = min(args['limit'], 10)
        res = Checkins.get(g.user.id, limit)
        return res, 200

    @api.doc(parser=post_checkin_parser,
             responses={404: "No slot existing with this id", 201: "Resource created"})
    @api.secure
    def post(self):
        """
        Add a new checkin
        """
        args = post_checkin_parser.parse_args()
        ok = Checkins.add(g.user.id, args['slot_id'])
        if not ok:
            api.abort(404, "No slot existing with this id")
        return "Resource created", 201


update_profile_parser = deepcopy(api_key_parser)
update_profile_parser.add_argument('email', type=str, location='form', help='user email')
update_profile_parser.add_argument('password', type=str, location='form', help='user password')
update_profile_parser.add_argument('name', type=unicode, location='form', help='user name')
update_profile_parser.add_argument('gender', type=str, location='form', help='gender')
update_profile_parser.add_argument('birthyear', type=int, location='form', help='birth year')
update_profile_parser.add_argument('picture', type=str, location='form', help='profile picture URL')


@api.route('/user/profile')
class Profile(Resource):
    @api.secure
    @api.doc(parser=api_key_parser, model=user_model)
    def get(self):
        """Get informations about a user"""
        return g.user.json, 200

    @api.doc(parser=update_profile_parser, model=user_model)
    @api.secure
    def put(self):
        """Update user profile information"""
        args = update_profile_parser.parse_args()
        del args['X-API-KEY']
        return email_update(g.user, **args)


image_parser = deepcopy(api_key_parser)
image_parser.add_argument(
    'image_type', type=str, required=True, help='Either "avatar" or "report"',
    location='form')
image_parser.add_argument(
    'file_name', type=str, required=True, help='File name of the image to be uploaded',
    location='form')
s3_url_model = api.model('S3 URL', {
    'request_url': fields.String(),
    'access_url': fields.String()
})

@api.route('/image')
class Image(Resource):
    @api.secure
    @api.doc(parser=image_parser, model=s3_url_model)
    def post(self):
        """
        Generate an S3 URL for image submission
        """
        args = image_parser.parse_args()
        data = Images.generate_s3_url(args["image_type"], args["file_name"])
        return data, 200


report_parser = deepcopy(api_key_parser)
report_parser.add_argument(
    'slot_id', type=int, required=True, help='Slot identifier', location='form')

report_model = api.model('Report', {
    'lat': fields.String(),
    'long': fields.String(),
    'way_name': fields.String(),
    'slot_id': fields.String(),
    'user_id': fields.String(),
    'id': fields.String(),
    'created': fields.String(),
    'image_url': fields.String()
})


@api.route('/report')
class Report(Resource):
    @api.secure
    @api.doc(parser=report_parser, model=report_model)
    def post(self):
        """Submit a report about incorrect data"""
        args = report_parser.parse_args()
        Reports.add(g.user.id, args["slot_id"], args["file_type"])
        return report.json, 200
