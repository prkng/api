# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com
"""
from json import loads
from flask import render_template, jsonify, Blueprint, abort, current_app, redirect, flash, url_for, request
from wtforms import Form, TextField, validators
from flask.ext.login import LoginManager, login_user, logout_user, login_required, UserMixin, current_user
from jinja2 import TemplateNotFound
from geojson import FeatureCollection, Feature

from prkng.models import District


# admin blueprint
admin = Blueprint(
    'admin',
    __name__,
    url_prefix='/admin',
    template_folder='templates',
    static_folder='static'
)

adminlogin = LoginManager()
adminlogin.login_view = "admin.login"


class AdminUser(UserMixin):
    def get_id(self):
        """
        There's only one user but flask-login need it
        """
        return unicode(1)

user = AdminUser()


@adminlogin.user_loader
def load_user(userid):
    return user


def init_admin(app):
    """
    Initialize login manager extension into flask application
    """
    app.register_blueprint(admin)
    adminlogin.init_app(app)


class LoginForm(Form):
    username = TextField('Username', [validators.Length(min=4, max=25)])
    password = TextField('Password', [validators.Required()])


@admin.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated():
        return redirect(url_for("admin.index"))
    form = LoginForm(request.form)
    if request.method == 'POST' and form.validate():
        if form.username.data == current_app.config['ADMIN_USER'] and \
           form.password.data == current_app.config['ADMIN_PASS']:
            login_user(user)
            return redirect(url_for("admin.index"))
        else:
            flash('Bad username/password', 'error')
    return render_template("adminlogin.html")


@admin.route('/logout', endpoint='logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('admin.login'))


@admin.route('/', endpoint='index')
@login_required
def adminview():
    try:
        return render_template('admin.html')
    except TemplateNotFound:
        abort(404)


@admin.route('/district/<city>', methods=['GET'], endpoint='district')
@login_required
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


@admin.route(
    '/district/<string:city>/<int:district_id>',
    methods=['GET'],
    endpoint='checkins')
@login_required
def district_checkins(city, district_id):
    """
    Get a list of checkins inside this district
    """
    startdate = request.args['startdate']
    enddate = request.args['enddate']

    checkins = District.get_checkins(city, district_id, startdate, enddate)
    return jsonify(results=checkins), 200
