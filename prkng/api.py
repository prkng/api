# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

"""
from __future__ import unicode_literals
from functools import wraps
from time import strftime
from aniso8601 import parse_datetime

from flask import render_template, Response, current_app
from flask.ext.restplus import Api, Resource, fields
from flask.ext.login import current_user, login_user, logout_user, make_secure_token
from geojson import FeatureCollection, Feature

from .models import SlotsModel, UserAuth, User, Checkins
from .login import OAuthSignIn

GEOM_TYPES = ('Point', 'LineString', 'Polygon',
              'MultiPoint', 'MultiLineString', 'MultiPolygon')


class PrkngApi(Api):
    def __init__(self, **kwargs):
        super(PrkngApi, self).__init__(**kwargs)

    def secure(self, func):
        '''Enforce authentication'''
        @wraps(func)
        def wrapper(*args, **kwargs):
            if current_user.is_authenticated():
                return func(*args, **kwargs)
            if current_app.login_manager._login_disabled:
                return func(*args, **kwargs)

            return self.abort(403, 'Not Authenticated')

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
    'type': fields.String(description='The GeoJSON Type', required=True, enum=GEOM_TYPES),
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
    'agenda': AgendaView(
        description='''list of days when the restriction apply (1: monday, ..., 7: sunday)
                       containing a list of time ranges when the restriction apply''',
        required=True),
    'special_days': fields.String(required=True),
    'restrict_typ': fields.String(
        description='special restriction details',
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
def timestamp(x):
    return parse_datetime(x).isoformat(str('T'))


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
        Backdoor to view results on a map
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


@api.route('/login/facebook')
class LoginFacebook(Resource):
    """
    Login with your facebook account
    """
    def get(self):
        if not current_user.is_anonymous():
            return api.abort(403, "Already authenticated as {}".format(current_user.name))
        oauth = OAuthSignIn.get_provider('facebook')
        return oauth.authorize()


@api.route('/login/google')
class LoginGoogle(Resource):
    """
    Login with your google account
    """
    def get(self):
        if not current_user.is_anonymous():
            return api.abort(403, "Already authenticated as {}".format(current_user.name))
        oauth = OAuthSignIn.get_provider('google')
        return oauth.authorize()


# define the slot id parser
slot_id_parser = api.parser()
slot_id_parser.add_argument('slot_id', type=int, required=True, help='The slot id', location='form')


@api.route('/checkin')
class Checkin(Resource):
    @api.secure
    def get(self):
        """
        Get the list of last checkins. Default to 10 last checkins.
        """
        res = Checkins.get(current_user.id, 10)
        return res, 200

    @api.doc(
        parser=slot_id_parser,
        responses={404: "No slot existing with this id",
                   201: "Resource created"}
    )
    @api.secure
    def post(self):
        """
        Add a new checkin
        """
        args = slot_id_parser.parse_args()
        ok = Checkins.add(current_user.id, args['slot_id'])
        if not ok:
            api.abort(404, "No slot existing with this id")
        return "Resource created", 201


@api.route('/logout')
class Logout(Resource):
    @api.secure
    def get(self):
        logout_user()
        return 'User logged out', 200


@api.route('/me')
class Profile(Resource):
    @api.secure
    def get(self):
        return User.get_profile(current_user.id), 200


@api.route('/callback/<provider>', endpoint='oauth_callback')
@api.hide
class OauthCallback(Resource):
    def get(self, provider):
        if not current_user.is_anonymous():
            return api.abort(403, "Already authenticated as {}".format(current_user.name))
        oauth = OAuthSignIn.get_provider(provider)
        auth_id, name, email, gender, fullprofile = oauth.callback()
        if auth_id is None:
            return api.abort(401, "Authentication failed.")
        user = UserAuth.get_user(auth_id)
        if not user:
            # add user auth informations and get the User associated
            user = UserAuth.add_userauth(
                name=name,
                auth_id=auth_id,
                email=email,
                auth_type=oauth.provider_name,
                fullprofile=fullprofile
            )

        # login user (powered by flask-login)
        login_user(user, True)
        return make_secure_token(name, email, auth_id)
