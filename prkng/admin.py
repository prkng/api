# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
import json
import time

from functools import wraps

from itsdangerous import TimedJSONWebSignatureSerializer, SignatureExpired, BadSignature
from flask import jsonify, Blueprint, abort, current_app, request
from jinja2 import TemplateNotFound
from geojson import FeatureCollection, Feature

from prkng.models import District, Checkins, Reports, City


def add_cors_to_response(resp):
    resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin','*')
    resp.headers['Access-Control-Allow-Credentials'] = 'true'
    resp.headers['Access-Control-Allow-Methods'] = 'PATCH, PUT, POST, OPTIONS, GET, DELETE'
    resp.headers['Access-Control-Allow-Headers'] = 'Authorization, Origin, X-Requested-With, Accept, DNT, Cache-Control, Accept-Encoding, Content-Type'
    return resp


admin = Blueprint('admin', __name__, url_prefix='/admin')
admin.after_request(add_cors_to_response)


def init_admin(app):
    """
    Initialize login manager extension into flask application
    """
    app.register_blueprint(admin)


def auth_required():
    def wrapper(func):
        @wraps(func)
        def decorator(*args, **kwargs):
            v = verify()
            if v:
                return v
            return func(*args, **kwargs)
        return decorator
    return wrapper


def create_token():
    iat = time.time()
    payload = {
        "iss": current_app.config["ADMIN_USER"],
        "iat": iat,
        "exp": iat + 21600
    }
    tjwss = TimedJSONWebSignatureSerializer(secret_key=current_app.config["SECRET_KEY"],
        expires_in=21600, algorithm_name="HS256")
    return tjwss.dumps(payload).decode("utf-8")


def verify():
    token = request.headers.get("Authorization", None)
    if not token:
        return "Authorization required", 401

    token = token.split()
    if token[0] != "Bearer" or len(token) > 2:
        return "Malformed token", 400
    token = token[1]

    try:
        tjwss = TimedJSONWebSignatureSerializer(secret_key=current_app.config["SECRET_KEY"],
            expires_in=3600, algorithm_name="HS256")
        payload = tjwss.loads(token)
    except SignatureExpired:
        return "Token expired", 401
    except BadSignature:
        return "Malformed token signature", 401


@admin.route('/token', methods=['POST'])
def generate_token():
    """
    Generate a JSON Web Token for use with Ember.js admin
    """
    data = json.loads(request.data)
    if data.get("username") == current_app.config["ADMIN_USER"] \
    and data.get("password") == current_app.config["ADMIN_PASS"]:
        return jsonify(token=create_token())
    else:
        return "Authorization required", 401


@admin.route('/')
def adminview():
    try:
        return render_template('admin.html')
    except TemplateNotFound:
        abort(404)


@admin.route('/district/<city>', methods=['GET'])
@auth_required()
def district(city):
    geojson = District.get(city)

    return jsonify(FeatureCollection([
        Feature(
            id=geo.id,
            geometry=loads(geo.geom),
            properties={
                "name": geo.name,
            }
        )
        for geo in geojson
    ])), 200


@admin.route('/checkins', methods=['GET'])
@auth_required()
def district_checkins():
    """
    Get a list of checkins
    """
    startdate = request.args.get('startdate', None)
    enddate = request.args.get('enddate', None)
    city = request.args.get('city', 'montreal')
    district = request.args.get('district', None)
    if district:
        checkins = District.get_checkins(city, district, startdate, enddate)
    else:
        checkins = City.get_checkins(city, startdate, enddate)
    return jsonify(checkins=checkins), 200


@admin.route('/reports', methods=['GET'])
@auth_required()
def get_reports():
    """
    Get a list of reports
    """
    city = request.args.get('city', 'montreal')
    district = request.args.get('district', None)
    if district:
        reports = District.get_reports(city, district)
    else:
        reports = City.get_reports(city)
    return jsonify(reports=reports), 200


@admin.route('/reports/<int:id>', methods=['DELETE'])
@auth_required()
def delete_report(id):
    """
    Delete a report from the database
    """
    Reports.delete(id)
    return "Resource deleted", 204
