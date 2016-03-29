from prkng.models import ParkingLots, Slots

from flask import jsonify, Blueprint, request, send_from_directory
import os


explorer = Blueprint('explorer', __name__, url_prefix='/explorer')

slot_props = (
    'id',
    'geojson',
    'rules',
    'button_locations',
    'way_name'
)


def init_explorer(app):
    """
    Initialize Explorer extension into Flask application
    """
    app.register_blueprint(explorer)


@explorer.route('/', defaults={'path': None})
@explorer.route('/<path:path>')
def test_view(path):
    """
    Serve explorer interface.
    Should only be used for testing; otherwise serve with NGINX instead.
    """
    if path and not path.startswith(("assets", "public", "fonts", "images")):
        path = None
    sdir = os.path.dirname(os.path.realpath(__file__))
    if path and path.startswith("images"):
        sdir = os.path.abspath(os.path.join(sdir, '../../../explorer/public'))
    else:
        sdir = os.path.abspath(os.path.join(sdir, '../../../explorer/dist'))
    return send_from_directory(sdir, path or 'index.html')


@explorer.route('/api/slots')
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
        request.args.get('duration', 0.25),
        int(request.args.get('type', 0)),
        request.args.get('invert') in [True, "true"]
    )
    if res == False:
        return jsonify(status="no feature found"), 404

    props = ["id", "geojson", "button_locations", "restrict_types"]
    slots = [
        {field: row[field] for field in props}
        for row in res
    ]

    return jsonify(slots=slots), 200


@explorer.route('/api/slots/<int:id>')
def get_slot(id):
    """
    Returns data on a specific slot
    """
    res = Slots.get_byid(id, slot_props)
    if not res:
        return jsonify(status="feature not found"), 404

    slot = {field: res[0][num] for num, field in enumerate(slot_props)}
    return jsonify(slot=slot), 200


@explorer.route('/api/lots')
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
