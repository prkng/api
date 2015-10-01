from prkng.api import auth_required, create_token
from prkng.analytics import Analytics
from prkng.models import Checkins, City, Corrections, FreeSpaces, ParkingLots, Reports, Slots, User
from prkng.notifications import schedule_notifications

from flask import jsonify, Blueprint, abort, current_app, request, send_from_directory
from geojson import Feature, FeatureCollection
import json
import os


admin = Blueprint('admin', __name__, url_prefix='/admin')

slot_props = (
    'id',
    'geojson',
    'rules',
    'button_locations',
    'way_name'
)


def init_admin(app):
    """
    Initialize login manager extension into flask application
    """
    app.register_blueprint(admin)


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
        sdir = os.path.abspath(os.path.join(sdir, '../../../prkng-admin/public'))
    else:
        sdir = os.path.abspath(os.path.join(sdir, '../../../prkng-admin/dist'))
    return send_from_directory(sdir, path or 'index.html')


@admin.route('/api/token', methods=['POST'])
def generate_token():
    """
    Generate a JSON Web Token for use with Ember.js admin
    """
    data = request.get_json()
    uname, passwd = data.get("username"), data.get("password")
    if uname in current_app.config["ADMIN_ACCTS"] \
    and passwd == current_app.config["ADMIN_ACCTS"][uname]:
        return jsonify(token=create_token(uname))
    else:
        return jsonify(message="Username or password incorrect"), 401


@admin.route('/api/checkins', methods=['GET'])
@auth_required()
def get_checkins():
    """
    Get a list of checkins
    """
    city = request.args.get('city', 'montreal')
    start = request.args.get('start')
    end = request.args.get('end')
    checkins = City.get_checkins(city, start, end)
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
    data = request.get_json()["report"]
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
    data = request.get_json()["correction"]
    corr = Corrections.add(data["slot_id"], "XX-"+data["code"],
        data["city"], data["description"], data["initials"],
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
    res = Slots.get_boundbox(
        request.args['neLat'],
        request.args['neLng'],
        request.args['swLat'],
        request.args['swLng'],
        slot_props,
        request.args.get('checkin'),
        request.args.get('duration'),
        int(request.args.get('type', 0)),
        request.args.get('invert') in [True, "true"]
    )
    if res == False:
        return jsonify(status="no feature found"), 404

    props = ["id", "geojson", "button_locations", "restrict_typ"]
    slots = [
        {field: row[field] for field in props}
        for row in res
    ]

    return jsonify(slots=slots), 200


@admin.route('/api/lots')
@auth_required()
def get_lots():
    """
    Returns garages inside a boundbox
    """
    res = ParkingLots.get_boundbox(
        request.args['neLat'],
        request.args['neLng'],
        request.args['swLat'],
        request.args['swLng']
    )
    if res == False:
        return jsonify(status="no feature found"), 404

    lots = [
        {key: value for key, value in row.items()}
        for row in res
    ]

    return jsonify(lots=lots), 200


@admin.route('/api/frees', methods=['GET'])
@auth_required()
def get_freed_spaces():
    """
    Get freed spaces
    """
    frees = FreeSpaces.get(request.args.get('minutes', 5))
    return jsonify(frees=frees), 200


@admin.route('/api/analytics', methods=['GET'])
@auth_required()
def get_analytics():
    """
    Get user and checkin analytics data
    """
    user = Analytics.get_user_data()
    acts = Analytics.get_active_user_data()
    actchks = Analytics.get_active_user_chk_data()
    chks = Analytics.get_checkin_data()
    return jsonify(users=user, actives=acts, activechks=actchks, checkins=chks), 200


@admin.route('/api/heatmap', methods=['GET'])
@auth_required()
def get_heatmap():
    """
    Get map usage heatmap
    """
    usage = Analytics.get_map_usage(request.args.get('hours', 24))
    return jsonify(heatmap=usage), 200


@admin.route('/api/notification', methods=['POST'])
@auth_required()
def send_apns():
    """
    Send push notifications by user ID
    """
    device_ids = {"ios": [], "android": []}
    data = request.get_json()
    if data["user_ids"]:
        for x in data["user_ids"]:
            u = User.get(x)
            if u and u.device_id:
                device_ids[u.device_type].append(u.device_id)
        schedule_notifications("ios", device_ids["ios"], data.get('text'))
        schedule_notifications("android", device_ids["android"], data.get('text'))
        return jsonify(device_ids=device_ids), 200
    else:
        return "No user IDs supplied", 400
