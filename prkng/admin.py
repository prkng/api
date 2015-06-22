# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
import json
import os
import time

from functools import wraps

from itsdangerous import TimedJSONWebSignatureSerializer, SignatureExpired, BadSignature
from flask import jsonify, Blueprint, abort, current_app, request, send_from_directory

from prkng.models import Checkins, Reports, City, Corrections, SlotsModel


admin = Blueprint('admin', __name__, url_prefix='/admin')


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


def create_token(user):
    iat = time.time()
    payload = {
        "iss": user,
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
            expires_in=21600, algorithm_name="HS256")
        payload = tjwss.loads(token)
    except SignatureExpired:
        return "Token expired", 401
    except BadSignature:
        return "Malformed token signature", 401


@admin.route('/', defaults={'path': None})
@admin.route('/<path:path>')
def test_view(path):
    """
    Serve admin interface.
    Should only be used for testing; otherwise serve with NGINX instead.
    """
    if path and not path.startswith(("assets", "public", "fonts", "images")):
        path = None
    sdir = os.path.dirname(os.path.realpath(__file__))
    if path and path.startswith("images"):
        sdir = os.path.abspath(os.path.join(sdir, '../../prkng-admin/public'))
    else:
        sdir = os.path.abspath(os.path.join(sdir, '../../prkng-admin/dist'))
    return send_from_directory(sdir, path or 'index.html')


@admin.route('/api/token', methods=['POST'])
def generate_token():
    """
    Generate a JSON Web Token for use with Ember.js admin
    """
    data = json.loads(request.data)
    if data.get("username") == current_app.config["ADMIN_USER"] \
    and data.get("password") == current_app.config["ADMIN_PASS"]:
        return jsonify(token=create_token(current_app.config["ADMIN_USER"]))
    else:
        return jsonify(message="Username or password incorrect"), 401


@admin.route('/api/checkins', methods=['GET'])
@auth_required()
def get_checkins():
    """
    Get a list of checkins
    """
    city = request.args.get('city', 'montreal')
    checkins = City.get_checkins(city)
    return jsonify(checkins=checkins), 200


@admin.route('/api/reports', methods=['GET'])
@auth_required()
def get_reports():
    """
    Get a list of reports
    """
    city = request.args.get('city', 'montreal')
    reports = City.get_reports(city)
    return jsonify(reports=reports), 200


@admin.route('/api/reports/<int:id>', methods=['GET'])
@auth_required()
def get_report(id):
    """
    Get an individual report
    """
    report = Reports.get(id)
    return jsonify(report=report), 200


@admin.route('/api/reports/<int:id>', methods=['PUT'])
@auth_required()
def update_report(id):
    """
    Updates a report's processing status
    """
    data = json.loads(request.data)["report"]
    report = Reports.set_progress(id, data["progress"])
    return jsonify(report=report), 200


@admin.route('/api/reports/<int:id>', methods=['DELETE'])
@auth_required()
def delete_report(id):
    """
    Delete a report from the database
    """
    Reports.delete(id)
    return "", 204


@admin.route('/api/corrections', methods=['GET'])
@auth_required()
def get_corrections():
    """
    Get all corrections made on slots by city
    """
    city = request.args.get('city', 'montreal')
    corrs = City.get_corrections(city)
    return jsonify(corrections=corrs), 200


@admin.route('/api/corrections/<int:id>', methods=['GET'])
@auth_required()
def get_correction(id):
    """
    Get a specific correction by its id
    """
    corr = Corrections.get(id)
    if not corr:
        return jsonify(message="No such correction found"), 404
    return jsonify(correction=corr), 200


@admin.route('/api/corrections', methods=['POST'])
@auth_required()
def add_correction():
    """
    Add a new correction for a slot
    """
    data = json.loads(request.data)["correction"]
    corr = Corrections.add(data["slot_id"], "XX-"+data["code"],
        data["city"], data["description"],
        data.get("season_start", ""), data.get("season_end", ""),
        data.get("time_max_parking", 0.0), json.dumps(data["agenda"]),
        data.get("special_days", ""), data.get("restrict_typ", ""))
    return jsonify(correction=corr), 201


@admin.route('/api/corrections/<int:id>', methods=['DELETE'])
@auth_required()
def delete_correction(id):
    """
    Remove a correction from the database
    If no restrictions for this slot remain, they will revert to city's values
    at next database process.
    """
    Corrections.delete(id)
    return "", 204


@admin.route('/api/corrections/apply', methods=['POST'])
@auth_required()
def apply_corrections():
    """
    Apply all pending corrections to proper slots
    """
    Corrections.apply()
    return jsonify(message="Operation successful"), 200


@admin.route('/api/slots')
@auth_required()
def get_slots():
    """
    Returns slots inside a boundbox
    """
    res = SlotsModel.get_all_within(
        request.args['neLat'],
        request.args['neLng'],
        request.args['swLat'],
        request.args['swLng']
    )

    slots = [
        {key: value for key, value in row.items()}
        for row in res
    ]

    return jsonify(slots=slots), 200
