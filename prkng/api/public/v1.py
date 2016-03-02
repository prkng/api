from __future__ import unicode_literals

from prkng.api.public import api
from prkng.models import Analytics, Carshares, Checkins, City, Images, ParkingLots, Reports, Slots, User, UserAuth
from prkng.login import facebook_signin, google_signin, email_register, email_signin, email_update
from prkng.tasks.general import parking_panda_welcome_email
from prkng.utils import timestamp

import copy
from geojson import loads, FeatureCollection, Feature
from flask import Response, g, request
from flask.ext.restplus import Resource, fields
from rq import Queue
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
    "restrict_typ": x["restrict_types"][0] if len(x["restrict_types"]) else None,
    "restrict_types": x["restrict_types"],
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
geometry_point = api.model('GeometryPoint', {
    'type': fields.String(required=True, enum=['Point']),
    'coordinates': fields.List(
        fields.Float,
        description='The geometry as coordinates lists',
        required=True),
})

geometry_linestring = api.model('GeometryLinestring', {
    'type': fields.String(required=True, enum=['LineString']),
    'coordinates': fields.List(
        fields.List(fields.Float),
        description='The geometry as coordinates lists',
        required=True),
})

agenda_view = api.model('AgendaView', {
    '%s' % day: fields.List(fields.List(fields.Float))
    for day in range(1, 8)
})

button_locations = api.model('ButtonLocations', {
    'long': fields.Float(),
    'lat': fields.Float()
})

rules_field = api.model('RulesField', {
    'code': fields.String(
        description='rule ID',
        required=True),
    'address': fields.String(
        description='street name',
        required=True),
    'description': fields.String(
        description='description of the parking rule',
        required=True),
    'season_start': fields.String(
        description='when the permission begins in the year (ex: 12-01 for december 1)',
        required=True),
    'season_end': fields.String(
        description='when the permission no longer applies',
        required=True),
    'time_max_parking': fields.Integer(
        description='restriction on parking time (minutes)',
        required=True),
    'agenda': fields.Nested(
        agenda_view,
        description='''list of days when the restriction apply (1: monday, ..., 7: sunday)
                       containing a list of time ranges when the restriction apply''',
        required=True),
    'permit_no': fields.String(
        description='city parking permit number applicable for this slot',
        required=True),
    'special_days': fields.String(required=True),
    'restrict_types': fields.List(
        fields.String,
        description='special restriction details',
        required=True),
    'paid_hourly_rate': fields.Float(
        description='hourly cost for paid parking here (if applicable)')
})

slots_field = api.model('SlotsField', {
    'way_name': fields.String,
    'button_locations': fields.List(fields.Nested(button_locations), required=True),
    'restrict_types': fields.List(fields.String),
    'compact': fields.Boolean(True)
})

slots_field_full = api.model('SlotsFieldFull', {
    'way_name': fields.String,
    'rules': fields.List(fields.Nested(rules_field)),
    'button_locations': fields.List(fields.Nested(button_locations), required=True),
    'compact': fields.Boolean(False)
})

slots_fields = api.model('v1SlotsGeoJSONFeature', {
    'id': fields.Integer(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': fields.Nested(geometry_linestring),
    'properties': fields.Nested(slots_field)
})

slots_fields_full = api.model('v1SlotFullGeoJSONFeature', {
    'id': fields.Integer(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': fields.Nested(geometry_linestring),
    'properties': fields.Nested(slots_field_full)
})

slots_collection_fields = api.model('v1SlotsGeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': fields.List(fields.Nested(slots_fields))
})


lot_attributes = api.model('LotAttributes', {
    'card': fields.Boolean,
    'valet': fields.Boolean,
    'indoor': fields.Boolean,
    'handicap': fields.Boolean
})

lot_street_view = api.model('LotStreetView', {
    'id': fields.String,
    'head': fields.Float
})

carshares_field = api.model('CarsharesField', {
    'company': fields.String(
        description='name of carshare operator',
        required=True),
    'partner_id': fields.String(
        description='DB identifier with the carshare operator',
        required=True),
    'name': fields.String(
        description='name of car (usually licence plate)',
        required=True),
    'vin': fields.String(
        description='VIN number of the car (if available)',
        required=True),
    'electric': fields.Boolean(
        description='True if car is recognized as an EV',
        required=True),
    'until': fields.DateTime(
        description='time the vehicle is available until (if applicable)',
        required=True),
    'fuel': fields.Integer(
        description='Percentage of fuel remaining in vehicle (null = unknown)')
})

carshare_lots_field = api.model('CarshareLotsField', {
    'company': fields.String(
        description='name of carshare operator',
        required=True),
    'name': fields.String(
        description='name of car (usually licence plate)',
        required=True),
    'capacity': fields.Integer(
        description='Max capacity of carshares in this lot'),
    'available': fields.Integer(
        description='Spaces currently available for carshares in this lot')
})

lot_agenda_view_object = api.model('LotAgendaViewObject', {
    'max': fields.Float,
    'daily': fields.Float,
    'hourly': fields.Float,
    'hours': fields.List(fields.Float)
})

lot_agenda_view = api.model('LotAgendaView', {
    '%s' % day: fields.List(fields.Nested(lot_agenda_view_object))
    for day in range(1, 8)
})

lots_field = api.model('LotsField', {
    'name': fields.String(
        description='name of parking lot / operator',
        required=True),
    'city': fields.String(
        description='city name',
        required=True),
    'partner_id': fields.String(
        description='ID of this lot for partner mgmt',
        required=True),
    'partner_name': fields.String(
        description='name of partner for mgmt',
        required=True),
    'operator': fields.String(
        description='name of parking lot operator',
        required=True),
    'capacity': fields.Integer(
        description='total number of spaces in the lot',
        required=True),
    'available': fields.Integer(
        description='number of spaces currently available in the lot',
        required=True),
    'address': fields.String(
        description='street address of lot entrance',
        required=True),
    'agenda': fields.Nested(lot_agenda_view,
        description='''list of days when the restriction apply (1: monday, ..., 7: sunday)
                       containing a list of time ranges when the restriction apply''',
        required=True),
    'attrs': fields.Nested(
        lot_attributes,
        description='list of amenities present at this lot/garage',
        required=True),
    'street_view': fields.Nested(
        lot_street_view,
        description='street view data object',
        required=True)
})


service_areas_version = api.model('ServiceAreasVersion', {
    'version': fields.String(description='version of resource'),
    'kml_addr': fields.String(description='URL to service areas dataset (KML format, gzipped)'),
    'geojson_addr': fields.String(description='URL to service areas dataset (GeoJSON format, gzipped)'),
    'kml_mask_addr': fields.String(description='URL to service areas mask dataset (KML format, gzipped)'),
    'geojson_mask_addr': fields.String(description='URL to service areas mask dataset (GeoJSON format, gzipped)')
})

service_areas_versions = api.model('ServiceAreasVersions', {'0': fields.Nested(service_areas_version)})

service_areas_model = api.model('ServiceAreasMeta', {
    'latest_version': fields.Integer(description='latest available version of resources'),
    'versions': fields.Nested(service_areas_versions)
})


@ns.route('/areas', endpoint='servicearea_v1')
class AreaAssets(Resource):
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


cities_fields = api.model('CitiesFields', {
    'id': fields.Integer(required=True),
    'name': fields.String(required=True),
    'display_name': fields.String(required=True),
    'lat': fields.Float(required=True),
    'long': fields.Float(required=True),
    'urban_area_radius': fields.Integer(required=True),
})


@ns.route('/cities', endpoint='cities_v1')
class Cities(Resource):
    @api.marshal_list_with(cities_fields)
    def get(self):
        """
        Returns coverage area information
        """
        return City.get_all(), 200


permits_fields = api.model('PermitsFields', {
    'id': fields.Integer(required=True),
    'city': fields.String(required=True),
    'permit': fields.String(required=True),
    'residential': fields.Boolean(required=True)
})

permits_parser = copy.deepcopy(api_key_parser)
permits_parser.add_argument(
    'city',
    type=str,
    location='args',
    required=True,
    help='City to get permits for'
)
permits_parser.add_argument(
    'residential',
    type=str,
    location='args',
    required=False,
    default='false',
    help='Only return residential permits'
)


@ns.route('/permits', endpoint='permits_v1')
class Permits(Resource):
    @api.secure
    @api.marshal_list_with(permits_fields)
    @api.doc(security='apikey', parser=permits_parser)
    def get(self):
        """
        Returns supported parking permits for a given city
        """
        args = permits_parser.parse_args()
        return City.get_permits(args['city'], args['residential'] in ['true', 'True', True]), 200


slot_parser = api.parser()
slot_parser.add_argument(
    'filter',
    type=str,
    location='args',
    default='true',
    help='Remove restrictions that do not apply from rules'
)
slot_parser.add_argument(
    'checkin',
    type=timestamp,
    location='args',
    default=time.strftime("%Y-%m-%dT%H:%M:%S"),
    help="Check-in timestamp in ISO 8601 ('2013-01-01T12:00'); default is now"
)
slot_parser.add_argument(
    'permit',
    type=str,
    location='args',
    default=False,
    help='Show permit restrictions for the specified number(s) as available'
)


@ns.route('/slots/<string:id>', endpoint='slot_v1')
class SlotResource(Resource):
    @api.marshal_with(slots_fields_full)
    @api.doc(
        params={'id': 'slot id'},
        responses={404: "feature not found"}
    )
    def get(self, id):
        """
        Returns the parking slot corresponding to the id
        """
        args = slot_parser.parse_args()
        args['filter'] = args['filter'] not in ['false', 'False', False]

        res = Slots.get_byid(id, slot_props, args['filter'], args['checkin'], args['permit'])
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


slots_parser = copy.deepcopy(api_key_parser)
slots_parser.add_argument(
    'radius',
    type=int,
    location='args',
    default=300,
    help='Radius search in meters; default is 300'
)
slots_parser.add_argument(
    'latitude',
    type=float,
    location='args',
    required=True,
    help='Latitude in degrees (WGS84)'
)
slots_parser.add_argument(
    'longitude',
    type=float,
    location='args',
    required=True,
    help='Longitude in degrees (WGS84)'
)
slots_parser.add_argument(
    'checkin',
    type=timestamp,
    location='args',
    default=time.strftime("%Y-%m-%dT%H:%M:%S"),
    help="Check-in timestamp in ISO 8601 ('2013-01-01T12:00'); default is now"
)
slots_parser.add_argument(
    'duration',
    type=float,
    location='args',
    default=0.5,
    help='Desired Parking time in hours; default is 0.5'
)
slots_parser.add_argument(
    'carsharing',
    type=str,
    location='args',
    default=False,
    help='Filter automatically by carsharing rules'
)
slots_parser.add_argument(
    'compact',
    type=str,
    location='args',
    default=False,
    help='Return only IDs, types and geometries for slots'
)
slots_parser.add_argument(
    'permit',
    type=str,
    location='args',
    default=False,
    help='Show permit restrictions for the specified number(s) as available'
)


@ns.route('/slots', endpoint='slots_v1')
class SlotsResource(Resource):
    @api.secure
    @api.marshal_with(slots_collection_fields)
    @api.doc(security='apikey',
        responses={404: "no feature found"}
    )
    @api.doc(parser=slots_parser)
    def get(self):
        """
        Returns slots around the point defined by (x, y)
        """
        args = slots_parser.parse_args()
        args['compact'] = args['compact'] not in ['false', 'False', False]
        args['carsharing'] = args['carsharing'] not in ['false', 'False', False]

        # push map search data to analytics
        Analytics.add_pos_tobuf("slots", g.user.id, args["latitude"],
            args["longitude"], args["radius"])

        city = City.get(args['longitude'], args['latitude'])
        if not city:
            api.abort(404, "no feature found")

        res = Slots.get_within(
            city,
            args['longitude'],
            args['latitude'],
            args['radius'],
            args['duration'],
            slot_props,
            args['checkin'],
            args['permit'],
            args['carsharing']
        )

        return FeatureCollection([
            Feature(
                id=feat['id'],
                geometry=feat['geojson'],
                properties=cpt_props(feat) if args['compact'] else nrm_props(feat)
            )
            for feat in res
        ]), 200


parking_lot_parser = copy.deepcopy(api_key_parser)
parking_lot_parser.add_argument(
    'latitude',
    type=float,
    location='args',
    help='Latitude in degrees (WGS84)'
)
parking_lot_parser.add_argument(
    'longitude',
    type=float,
    location='args',
    help='Longitude in degrees (WGS84)'
)
parking_lot_parser.add_argument(
    'radius',
    type=int,
    location='args',
    default=300,
    help='Radius search in meters; default is 300'
)
parking_lot_parser.add_argument(
    'nearest',
    type=int,
    location='args',
    default=0,
    help='If no lots found in given radius, return nearest X lots to lat/long'
)
parking_lot_parser.add_argument(
    'partner_id',
    type=str,
    location='args',
    help='Return only the lot with the corresponding partner ID'
)

lots_fields = api.model('LotsFields', {
    'id': fields.Integer(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': fields.Nested(geometry_point),
    'properties': fields.Nested(lots_field)
})

lots_collection_fields = api.model('LotsGeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': fields.List(fields.Nested(lots_fields))
})


@ns.route('/lots', endpoint='parkinglots_v1')
class Lots(Resource):
    @api.secure
    @api.marshal_with(lots_collection_fields)
    @api.doc(security='apikey',
        responses={404: "no feature found"}
    )
    @api.doc(parser=parking_lot_parser)
    def get(self):
        """
        Return parking lots and garages around the point defined by (x, y)
        """
        args = parking_lot_parser.parse_args()
        if not args.get("latitude") and not args.get("longitude") and not args.get("partner_id"):
            return "Requires either lat/long or partner_id", 400

        if args.get("partner_id"):
            res = ParkingLots.get_bypartnerid(args["partner_id"])
        else:
            # push map search data to analytics
            Analytics.add_pos_tobuf("lots", g.user.id, args["latitude"],
                args["longitude"], args["radius"])

            city = City.get(args['longitude'], args['latitude'])
            if not city:
                api.abort(404, "no feature found")

            res = ParkingLots.get_within(args["longitude"], args["latitude"],
                args["radius"])

            if not res and args["nearest"]:
                res = ParkingLots.get_nearest(args["longitude"], args["latitude"],
                    args["nearest"])

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


@ns.route('/lots/<string:id>', endpoint='parkinglot_v1')
class LotResource(Resource):
    @api.secure
    @api.marshal_with(lots_fields)
    @api.doc(security='apikey',
        params={'id': 'lot id'},
        responses={404: "feature not found"}
    )
    def get(self, id):
        """
        Returns the parking lot corresponding to the id
        """
        res = ParkingLots.get_byid(id)
        if not res:
            api.abort(404, "feature not found")

        res = res[0]
        return Feature(
            id=res[0],
            geometry=res[1],
            properties={
                field: res[num]
                for num, field in enumerate(ParkingLots.properties[2:], start=2)
            }
        ), 200



carshare_parser = copy.deepcopy(api_key_parser)
carshare_parser.add_argument(
    'radius',
    type=int,
    location='args',
    default=300,
    help='Radius search in meters; default is 300'
)
carshare_parser.add_argument(
    'latitude',
    type=float,
    location='args',
    required=True,
    help='Latitude in degrees (WGS84)'
)
carshare_parser.add_argument(
    'longitude',
    type=float,
    location='args',
    required=True,
    help='Longitude in degrees (WGS84)'
)
carshare_parser.add_argument(
    'company',
    type=str,
    location='args',
    required=False,
    help='Return carshares for a particular company (or companies, comma-separated) only'
)
carshare_parser.add_argument(
    'nearest',
    type=int,
    location='args',
    default=0,
    help='If no carshares found in given radius, return nearest X cars to lat/long'
)

carshares_fields = api.model('CarsharesGeoJSONFeature', {
    'id': fields.Integer(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': fields.Nested(geometry_point),
    'properties': fields.Nested(carshares_field)
})

carshares_collection_fields = api.model('CarsharesGeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': fields.List(fields.Nested(carshares_fields))
})


@ns.route('/carshares', endpoint='carshares_v1')
class CarsharesResource(Resource):
    @api.secure
    @api.marshal_with(carshares_collection_fields)
    @api.doc(security='apikey',
        responses={404: "no feature found"}
    )
    @api.doc(parser=carshare_parser)
    def get(self):
        """
        Return available carshares around the point defined by (x, y)
        """
        args = carshare_parser.parse_args()

        city = City.get(args['longitude'], args['latitude'])
        if not city:
            api.abort(404, "no feature found")

        res = Carshares.get_within(city, args['longitude'], args['latitude'], args['radius'],
            args['company'] or False)

        if not res and args["nearest"]:
            res = Carshares.get_nearest(city, args["longitude"], args["latitude"],
                args["nearest"], args['company'] or False)

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=feat[1],
                properties={
                    field: feat[num]
                    for num, field in enumerate(Carshares.select_properties[2:], start=2)
                }
            )
            for feat in res
        ]), 200


carshare_lots_fields = api.model('CarshareLotsGeoJSONFeature', {
    'id': fields.Integer(required=True),
    'type': fields.String(required=True, enum=['Feature']),
    'geometry': fields.Nested(geometry_point),
    'properties': fields.Nested(carshare_lots_field)
})

carshare_lots_collection_fields = api.model('CarshareLotsGeoJSONFeatureCollection', {
    'type': fields.String(required=True, enum=['FeatureCollection']),
    'features': fields.List(fields.Nested(carshare_lots_fields))
})


@ns.route('/carshare_lots', endpoint='carsharelots_v1')
class CarshareLotsResource(Resource):
    @api.secure
    @api.marshal_with(carshare_lots_collection_fields)
    @api.doc(security='apikey',
        responses={404: "no feature found"}
    )
    @api.doc(parser=carshare_parser)
    def get(self):
        """
        Return carshare lots and data around the point defined by (x, y)
        """
        args = carshare_parser.parse_args()

        city = City.get(args['longitude'], args['latitude'])
        if not city:
            api.abort(404, "no feature found")

        res = Carshares.get_lots_within(city, args['longitude'], args['latitude'], args['radius'],
            args['company'] or False)

        if not res and args["nearest"]:
            res = Carshares.get_lots_nearest(city, args["longitude"], args["latitude"],
                args["nearest"], args['company'] or False)

        return FeatureCollection([
            Feature(
                id=feat[0],
                geometry=feat[1],
                properties={
                    field: feat[num]
                    for num, field in enumerate(Carshares.lot_properties[2:], start=2)
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
login_parser.add_argument('name', type=str, location='form', help='Profile: Full Name (for Google logins only)', required=False)
login_parser.add_argument('picture', type=str, location='form', help='Profile: Profile Picture (for Google logins only)', required=False)

user_model = api.model('User', {
    'name': fields.String(),
    'first_name': fields.String(),
    'last_name': fields.String(),
    'email': fields.String(),
    'apikey': fields.String(),
    'created': fields.DateTime(),
    'auth_id': fields.String(),
    'device_id': fields.String(),
    'device_type': fields.String(),
    'lang': fields.String(),
    'last_hello': fields.DateTime(),
    'id': fields.Integer(),
    'gender': fields.String(),
    'image_url': fields.String(),
    'push_on_temp': fields.Boolean(),
    'sns_id': fields.String()
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

update_checkin_parser = copy.deepcopy(api_key_parser)
update_checkin_parser.add_argument(
    'is_hidden', type=str, default="false", help='Slot identifier', location='form')

checkin_model = api.model('Checkin', {
    'checkin_time': fields.DateTime(),
    'checkout_time': fields.DateTime(),
    'long': fields.Float(),
    'lat': fields.Float(),
    'city': fields.String(),
    'way_name': fields.String(),
    'slot_id': fields.Integer(),
    'user_id': fields.Integer(),
    'id': fields.Integer(),
    'active': fields.Boolean()
})


@ns.route('/checkins', endpoint='checkinlist_v1')
class CheckinList(Resource):
    @api.marshal_list_with(checkin_model)
    @api.doc(security='apikey', parser=get_checkin_parser,
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

    @api.doc(security='apikey', parser=post_checkin_parser, model=checkin_model,
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
             security='apikey', parser=update_checkin_parser,
             responses={200: "Resource updated"})
    @api.secure
    def put(self, id):
        """
        Modify an existing checkin.

        Presently only used to set `is_hidden` flag to True or False.
        """
        args = update_checkin_parser.parse_args()
        args['is_hidden'] = args['is_hidden'] not in ['false', 'False', False]
        Checkins.update(g.user.id, id, args['is_hidden'])
        return "Resource modified", 200

    @api.doc(params={'id': 'checkin id'},
             security='apikey', parser=api_key_parser,
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
    @api.doc(security='apikey', parser=api_key_parser, model=user_model)
    def get(self):
        """Get information about a user"""
        return g.user.json, 200

    @api.doc(security='apikey', parser=update_profile_parser, model=user_model)
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
    @api.doc(security='apikey', parser=image_parser, model=s3_url_model)
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
report_parser.add_argument('city', type=str, required=False, default='montreal',
    location='form', help='city name')
report_parser.add_argument('image_url', type=str, required=True,
    location='form', help='report image URL')
report_parser.add_argument('notes', type=str,
    location='form', help='report notes')


@ns.route('/reports', endpoint='report_v1')
class Report(Resource):
    @api.secure
    @api.doc(security='apikey', parser=report_parser,
        responses={201: "Resource created"})
    def post(self):
        """Submit a report about incorrect data"""
        args = report_parser.parse_args()
        city = args["city"]
        if not args["city"]:
            city = City.get(args['longitude'], args['latitude'])
        Reports.add(g.user.id, city, args.get("slot_id", None),
            args["longitude"], args["latitude"], args.get("image_url", ""),
            args.get("notes", ""))
        return "Resource created", 201


search_parser = copy.deepcopy(api_key_parser)
search_parser.add_argument(
    'query', type=str, required=True, help='Search query string', location='form')

@ns.route('/analytics/search', endpoint='search_v1')
class Search(Resource):
    @api.secure
    @api.doc(security='apikey', parser=search_parser,
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
    @api.doc(security='apikey', parser=event_parser,
        responses={201: "Resource created"})
    def post(self):
        """Send analytics event data"""
        args = event_parser.parse_args()

        # buffer map displacement analytics and feature selection
        # enter geofence arrival/departure data directly into database
        if "fence" in args["event"]:
            Analytics.add_event(g.user.id, args.get("latitude"), args.get("longitude"),
                args["event"])
        elif "parking_panda" in args["event"]:
            q = Queue('low', connection=Redis(db=1))
            q.enqueue(parking_panda_welcome_email, g.user.name, g.user.email)
            Analytics.add_event_tobuf(g.user.id, args.get("latitude"), args.get("longitude"),
                args["event"])
        else:
            Analytics.add_event_tobuf(g.user.id, args.get("latitude"), args.get("longitude"),
                args["event"])

        return "Resource created", 201


hello_parser = copy.deepcopy(api_key_parser)
hello_parser.add_argument(
    'device_type', type=str, required=True, help='Either `ios` or `android`', location='form')
hello_parser.add_argument(
    'device_id', type=str, required=True, help='Device ID', location='form')
hello_parser.add_argument(
    'lang', type=str, required=True, help='User\'s preferred language (`en`, `fr`)', location='form')
hello_parser.add_argument(
    'push_on_temp_restriction', type=str, default='false',
    help='Receive push notifications for dynamically-added restrictions (snow removal, etc)?')

@ns.route('/hello', endpoint='hello_v1')
class Hello(Resource):
    @api.secure
    @api.doc(security='apikey', parser=hello_parser,
        responses={200: "Hello there!"})
    def post(self):
        """Send analytics event data"""
        args = hello_parser.parse_args()
        args['push_on_temp_restriction'] = args['push_on_temp_restriction'] not in ['false', 'False', False]
        u = User.get(g.user.id)
        u.hello(args.get('device_type'), args.get('device_id'), args.get('lang'),
            args.get('push_on_temp_restriction'))
        return "Hello there!", 200
