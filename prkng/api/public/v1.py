from __future__ import unicode_literals

from prkng.api.public import api
from prkng.models import Analytics, Checkins, City, Images, ParkingLots, Reports, Slots, User, UserAuth
from prkng.login import facebook_signin, google_signin, email_register, email_signin, email_update
from prkng.utils import timestamp

import copy
from geojson import loads, FeatureCollection, Feature
from flask import render_template, Response, g, request
from flask.ext.restplus import Resource, fields
import time


GEOM_TYPES = ('Point', 'LineString', 'Polygon',
              'MultiPoint', 'MultiLineString', 'MultiPolygon')
ns = api.namespace('v1', 'API v1')

slot_props = (
    'id',
    'geojson',
    'rules',
    'button_locations',
    'way_name'
)

nrm_props = lambda x: {
    "button_locations": x["button_locations"],
    "rules": x["rules"],
    "way_name": x["way_name"],
    "compact": False
}

cpt_props = lambda x: {
    "button_locations": x["button_locations"],
    "restrict_typ": x["restrict_typ"],
    "way_name": x["way_name"],
    "compact": True
}


# define header parser for the API key
api_key_parser = api.parser()
api_key_parser.add_argument(
    'X-API-KEY',
    type=str,
    location='headers',
    help='Prkng API Key',
    required=True
)

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
class ButtonLocations(fields.Raw):
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
    'permit_no': fields.String(
        description='city parking permit number applicable for this slot',
        required=True),
    'special_days': fields.String(required=True),
    'restrict_typ': fields.String(
        description='special restriction details',
        required=True),
    'paid_hourly_rate': fields.Float(
        description='hourly cost for paid parking here (if applicable)'),
    'button_locations': fields.List(ButtonLocations(required=True))
})
class SlotsField(fields.Raw):
    pass


@api.model(fields={
    'indoor': fields.Boolean(),
    'clerk': fields.Boolean(),
    'valet': fields.Boolean()
})
class LotAttributes(fields.Raw):
    pass


@api.model(fields={
    'name': fields.String(
        description='name of parking lot / operator',
        required=True),
    'address': fields.String(
        description='street address of lot entrance',
        required=True),
    'agenda': AgendaView(
        description='list of days when lot/garage is open, containing a list of time ranges when open',
        required=True),
    'attrs': LotAttributes(
        description='list of amenities present at this lot/garage',
        required=True),
    'daily_price': fields.Float(
        description='price per day for a space in this lot/garage')
})
class LotsField(fields.Raw):
    pass


@api.model(fields={
    'version': fields.String(description='version of resource'),
    'kml_addr': fields.String(description='URL to service areas dataset (KML format, gzipped)'),
    'geojson_addr': fields.String(description='URL to service areas dataset (GeoJSON format, gzipped)'),
    'kml_mask_addr': fields.String(description='URL to service areas mask dataset (KML format, gzipped)'),
    'geojson_mask_addr': fields.String(description='URL to service areas mask dataset (GeoJSON format, gzipped)')
})
class ServiceAreasVersion(fields.Raw):
    pass


@api.model(fields={0: ServiceAreasVersion()})
class ServiceAreasVersions(fields.Raw):
    pass


service_areas_model = api.model('ServiceAreasMeta', {
    'latest_version': fields.Integer(description='latest available version of resources'),
    'versions': ServiceAreasVersions()
})


@ns.route('/areas', endpoint='servicearea_v1')
class ServiceAreas(Resource):
    @api.doc(model=service_areas_model)
    def get(self):
        """
        Returns coverage area package versions and metadata
        """
        res = City.get_assets()

        return {
            "latest_version": max([x["version"] for x in res]),
            "versions": {
                x["version"]: x for x in res
            }
        }, 200


slots_fields = api.model('v1SlotsGeoJSONFeature', {
    'id': fields.String(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': Geometry(required=True),
    'properties': SlotsField(required=True),
})

slots_collection_fields = api.model('v1SlotsGeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': api.as_list(fields.Nested(slots_fields))
})


@ns.route('/slots/<string:id>', endpoint='slot_v1')
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
        res = Slots.get_byid(id, slot_props)
        if not res:
            api.abort(404, "feature not found")

        res = res[0]
        return Feature(
            id=res[0],
            geometry=res[1],
            properties={
                field: res[num]
                for num, field in enumerate(slot_props[2:], start=2)
            }
        ), 200


slot_parser = copy.deepcopy(api_key_parser)
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
    default=time.strftime("%Y-%m-%dT%H:%M:%S"),
    help="Check-in timestamp in ISO 8601 ('2013-01-01T12:00'); default is now"
)
slot_parser.add_argument(
    'duration',
    type=float,
    location='args',
    default=0.5,
    help='Desired Parking time in hours; default is 0.5'
)
slot_parser.add_argument(
    'carsharing',
    type=str,
    location='args',
    default=False,
    help='Filter automatically by carsharing rules'
)
slot_parser.add_argument(
    'compact',
    type=str,
    location='args',
    default=False,
    help='Return only IDs, types and geometries for slots'
)


@ns.route('/slots', endpoint='slots_v1')
class SlotsResource(Resource):
    @api.secure
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
        args['carsharing'] = args['carsharing'] not in ['false', False]

        # push map search data to analytics
        Analytics.add_pos_tobuf("slots", g.user.id, args["latitude"],
            args["longitude"], args["radius"])

        res = Slots.get_within(
            args['longitude'],
            args['latitude'],
            args['radius'],
            24.0 if args['carsharing'] else args['duration'],
            slot_props,
            args['checkin'],
            not args['carsharing'],
            'all' if args['carsharing'] else False
        )
        if res == False:
            api.abort(404, "no feature found")

        return FeatureCollection([
            Feature(
                id=feat['id'],
                geometry=feat['geojson'],
                properties=cpt_props(feat) if args.get('compact') else nrm_props(feat)
            )
            for feat in res
        ]), 200


parking_lot_parser = copy.deepcopy(api_key_parser)
parking_lot_parser.add_argument(
    'latitude',
    type=float,
    location='args',
    required=True,
    help='Latitude in degrees (WGS84)'
)
parking_lot_parser.add_argument(
    'longitude',
    type=float,
    location='args',
    required=True,
    help='Longitude in degrees (WGS84)'
)

lots_fields = api.model('LotsGeoJSONFeature', {
    'id': fields.String(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': Geometry(required=True),
    'properties': LotsField(required=True),
})

lots_collection_fields = api.model('LotsGeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': api.as_list(fields.Nested(lots_fields))
})


@ns.route('/lots', endpoint='parkinglots_v1')
class Lots(Resource):
    @api.secure
    @api.marshal_list_with(lots_collection_fields)
    @api.doc(
        responses={404: "no feature found"}
    )
    @api.doc(parser=parking_lot_parser)
    def get(self):
        """
        Return parking lots and garages around the point defined by (x, y)
        """
        args = parking_lot_parser.parse_args()

        # push map search data to analytics
        Analytics.add_pos_tobuf("lots", g.user.id, args["latitude"],
            args["longitude"], 300)

        res = ParkingLots.get_all()

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=feat[1],
                properties={
                    field: feat[num]
                    for num, field in enumerate(ParkingLots.properties[2:], start=2)
                }
            )
            for feat in res
        ]), 200


login_parser = api.parser()
login_parser.add_argument('type', type=str, location='form', help='login type (facebook, google, etc). required for OAuth2', required=False)
login_parser.add_argument('email', type=str, location='form', help='user email (for email logins only)', required=False)
login_parser.add_argument('password', type=str, location='form', help='user password (for email logins only)', required=False)
login_parser.add_argument(
    'access_token',
    type=str,
    location='form',
    help='OAuth2 user access token (for facebook/google logins only)',
    required=False
)

user_model = api.model('User', {
    'name': fields.String(),
    'email': fields.String(),
    'apikey': fields.String(),
    'created': fields.String(),
    'auth_id': fields.String(),
    'id': fields.String(),
    'gender': fields.String(),
    'image_url': fields.String()
})


@ns.route('/login', endpoint='login_v1')
class Login(Resource):
    @api.doc(parser=login_parser, model=user_model)
    def post(self):
        """
        Login and receive an API key.
        """
        args = login_parser.parse_args()
        if (args.get("type") in ["facebook", "google"] and not args.get("access_token")) \
          or (args.get("access_token") and not args.get("type")):
            return "Authorization required", 401
        if args.get("type") == "facebook":
            return facebook_signin(args['access_token'])
        elif args.get("type") == "google":
            return google_signin(args['access_token'])
        else:
            return email_signin(args['email'], args['password'])


register_parser = api.parser()
register_parser.add_argument('email', required=True, type=str, location='form', help='user email')
register_parser.add_argument('password', required=True, type=str, location='form', help='user password')
register_parser.add_argument('name', required=True, type=unicode, location='form', help='user name')
register_parser.add_argument('gender', type=str, location='form', help='gender')
register_parser.add_argument('birthyear', type=str, location='form', help='birth year')
register_parser.add_argument('image_url', type=str, location='form', help='avatar URL')


@ns.route('/login/register', endpoint='register_v1')
class Register(Resource):
    @api.doc(parser=register_parser, model=user_model)
    def post(self):
        """
        Register a new account.
        """
        args = register_parser.parse_args()
        return email_register(**args)


passwd_reset_parser = api.parser()
passwd_reset_parser.add_argument(
    'email', type=str, required=True, help='Email of account', location='form')


@ns.route('/login/resetpass', endpoint='resetpass_v1')
class LoginResetPass(Resource):
    @api.doc(parser=passwd_reset_parser,
            responses={200: "OK", 400: "Account not found"})
    def post(self):
        """
        Send an account password reset code
        """
        args = passwd_reset_parser.parse_args()
        user = User.get_byemail(args["email"].lower())
        if not user:
            return "Account not found", 400
        return UserAuth.send_reset_code("email${}".format(user.id), user.email)


passwd_change_parser = api.parser()
passwd_change_parser.add_argument(
    'email', type=str, required=True, help='Email of account to reset', location='form')
passwd_change_parser.add_argument(
    'code', type=str, required=True, help='Account reset code', location='form')
passwd_change_parser.add_argument(
    'passwd', type=str, required=True, help='New password', location='form')


@ns.route('/login/changepass', endpoint='changepass_v1')
class LoginChangePass(Resource):
    @api.doc(parser=passwd_change_parser,
            responses={200: "OK", 404: "Account not found", 400: "Reset code incorrect"})
    def post(self):
        """
        Change an account's password via reset code
        """
        args = passwd_change_parser.parse_args()
        user = User.get_byemail(args["email"])
        if not user:
            return "Account not found", 404
        if not UserAuth.update_password("email${}".format(user.id), args["passwd"], args["code"]):
            return "Reset code incorrect", 400


# define the slot id parser
post_checkin_parser = copy.deepcopy(api_key_parser)
post_checkin_parser.add_argument(
    'slot_id', type=int, required=True, help='Slot identifier', location='form')

get_checkin_parser = copy.deepcopy(api_key_parser)
get_checkin_parser.add_argument(
    'limit', type=int, default=10, help='Slot identifier', location='query')

checkin_model = api.model('Checkin', {
    'checkin_time': fields.String(),
    'checkout_time': fields.String(),
    'long': fields.String(),
    'lat': fields.String(),
    'way_name': fields.String(),
    'slot_id': fields.Integer(),
    'id': fields.Integer(),
    'active': fields.Boolean()
})


@ns.route('/checkins', endpoint='checkinlist_v1')
class CheckinList(Resource):
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
        res = Checkins.get_all(g.user.id, limit)
        return res, 200

    @api.doc(parser=post_checkin_parser, model=checkin_model,
             responses={404: "No slot existing with this id", 201: "Resource created"})
    @api.secure
    def post(self):
        """
        Add a new checkin
        """
        args = post_checkin_parser.parse_args()
        res = Checkins.add(g.user.id, args['slot_id'])
        if not res:
            api.abort(404, "No slot existing with this id")
        return res, 201


@ns.route('/checkins/<string:id>', endpoint='checkins_v1')
class Checkin(Resource):
    @api.doc(params={'id': 'checkin id'},
             parser=api_key_parser,
             responses={204: "Resource deleted"})
    @api.secure
    def delete(self, id):
        """
        Deactivate an existing checkin
        """
        Checkins.remove(g.user.id, id)
        return "Resource deleted", 204


update_profile_parser = copy.deepcopy(api_key_parser)
update_profile_parser.add_argument('email', type=str, location='form', help='user email')
update_profile_parser.add_argument('password', type=str, location='form', help='user password')
update_profile_parser.add_argument('name', type=unicode, location='form', help='user name')
update_profile_parser.add_argument('gender', type=str, location='form', help='gender')
update_profile_parser.add_argument('birthyear', type=str, location='form', help='birth year')
update_profile_parser.add_argument('image_url', type=str, location='form', help='avatar URL')


@ns.route('/user/profile', endpoint='profile_v1')
class Profile(Resource):
    @api.secure
    @api.doc(parser=api_key_parser, model=user_model)
    def get(self):
        """Get information about a user"""
        return g.user.json, 200

    @api.doc(parser=update_profile_parser, model=user_model)
    @api.secure
    def put(self):
        """Update user profile information"""
        args = update_profile_parser.parse_args()
        del args['X-API-KEY']
        return email_update(g.user, **args)


image_parser = copy.deepcopy(api_key_parser)
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

@ns.route('/images', endpoint='image_v1')
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


report_parser = copy.deepcopy(api_key_parser)
report_parser.add_argument(
    'slot_id', type=int, help='Slot identifier', location='form')
report_parser.add_argument(
    'latitude',
    type=float,
    location='form',
    required=True,
    help='Latitude in degrees (WGS84)'
)
report_parser.add_argument(
    'longitude',
    type=float,
    location='form',
    required=True,
    help='Longitude in degrees (WGS84)'
)
report_parser.add_argument('image_url', type=str, required=True,
    location='form', help='report image URL')
report_parser.add_argument('notes', type=str,
    location='form', help='report notes')


@ns.route('/reports', endpoint='report_v1')
class Report(Resource):
    @api.secure
    @api.doc(parser=report_parser,
        responses={201: "Resource created"})
    def post(self):
        """Submit a report about incorrect data"""
        args = report_parser.parse_args()
        Reports.add(g.user.id, args.get("slot_id", None), args["longitude"],
            args["latitude"], args.get("image_url", ""), args.get("notes", ""))
        return "Resource created", 201


search_parser = copy.deepcopy(api_key_parser)
search_parser.add_argument(
    'query', type=str, required=True, help='Search query string', location='form')

@ns.route('/analytics/search', endpoint='search_v1')
class Search(Resource):
    @api.secure
    @api.doc(parser=search_parser,
        responses={201: "Resource created"})
    def post(self):
        """Send search query data"""
        args = search_parser.parse_args()
        Analytics.add_search(g.user.id, args["query"])
        return "Resource created", 201


event_parser = copy.deepcopy(api_key_parser)
event_parser.add_argument(
    'latitude',
    type=float,
    location='form',
    help='Latitude in degrees (WGS84)'
)
event_parser.add_argument(
    'longitude',
    type=float,
    location='form',
    help='Longitude in degrees (WGS84)'
)
event_parser.add_argument(
    'event', type=str, required=True, help='Event type, e.g. `enter_fence` or `leave_fence`',
    location='form')

@ns.route('/analytics/event', endpoint='event_v1')
class Event(Resource):
    @api.secure
    @api.doc(parser=event_parser,
        responses={201: "Resource created"})
    def post(self):
        """Send analytics event data"""
        args = event_parser.parse_args()

        # buffer map displacement analytics and feature selection
        # enter geofence arrival/departure data directly into database
        if "fence" in args["event"]:
            Analytics.add_event(g.user.id, args.get("latitude"), args.get("longitude"),
                args["event"])
        else:
            Analytics.add_event_tobuf(g.user.id, args.get("latitude"), args.get("longitude"),
                args["event"])

        # FIXME when we migrate to 1.3
        # checkout of the last spot and add departure time
        if args["event"] == "fence_response_yes":
            Checkins.remove(g.user.id, left=True)

        return "Resource created", 201


hello_parser = copy.deepcopy(api_key_parser)
hello_parser.add_argument(
    'device_type', type=str, required=True, help='Either `ios` or `android`', location='form')
hello_parser.add_argument(
    'device_id', type=str, required=True, help='Device ID', location='form')
hello_parser.add_argument(
    'lang', type=str, required=True, help='User\'s preferred language (`en`, `fr`)', location='form')

@ns.route('/hello', endpoint='hello_v1')
class Hello(Resource):
    @api.secure
    @api.doc(parser=hello_parser,
        responses={200: "Hello there!"})
    def post(self):
        """Send analytics event data"""
        args = hello_parser.parse_args()
        u = User.get(g.user.id)
        u.hello(args.get('device_type'), args.get('device_id'), args.get('lang'))
        return "Hello there!", 200
