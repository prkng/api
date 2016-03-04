from prkng.database import db, metadata
from prkng.utils import random_string

import boto.ses
import datetime
from flask import current_app
from flask.ext.login import UserMixin
from itsdangerous import JSONWebSignatureSerializer
from passlib.hash import pbkdf2_sha256
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, func, Index, Integer, String, Table, text
from sqlalchemy.dialects.postgresql import JSONB, ENUM
import time



AUTH_PROVIDERS = (
    'facebook',
    'google',
    'email'
)

user_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=False),
    Column('first_name', String),
    Column('last_name', String),
    Column('gender', String(10)),
    Column('email', String(60), index=True, unique=True, nullable=False),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('device_type', String, nullable=True),
    Column('device_id', String, nullable=True),
    Column('sns_id', String, nullable=True),
    Column('lang', String, nullable=True),
    Column('last_hello', DateTime, server_default=text('NOW()'), nullable=True),
    Column('push_on_temp', Boolean, default=False),
    Column('apikey', String),
    Column('image_url', String)
)

userauth_table = Table(
    'users_auth',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey("users.id"), index=True, nullable=False),
    Column('auth_id', String(1024), index=True, unique=True),  # id given by oauth provider
    Column('auth_type', ENUM(*AUTH_PROVIDERS, name='auth_provider')),  # oauth_type
    Column('password', String),  # for the email accounts
    Column('fullprofile', JSONB),
    Column('reset_code', String, nullable=True)
)

# creating a functional index on apikey field
user_api_index = Index(
    'idx_users_apikey',
    func.substr(user_table.c.apikey, 0, 6)
)

class User(UserMixin):
    """
    Subclassed UserMixin for the methods that Flask-Login expects user objects to have
    """
    def __init__(self, kwargs):
        super(UserMixin, self).__init__()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return u"<User {} : {}>".format(self.id, self.name)

    def update_apikey(self, newkey):
        """
        Update the user's API key credential in the database.

        :param newkey: the new API key to use (str)
        :returns: None
        """
        db.engine.execute("""
            update users set apikey = '{key}'
            where id = {user_id}
            """.format(key=newkey, user_id=self.id))
        self.apikey = newkey

    def update_profile(self, name=None, first_name=None, last_name=None, email=None,
            gender=None, image_url=None):
        """
        Update profile information.

        :param name: user's whole name concatenated (opt str)
        :param first_name: first name (opt str) if not given, defaults to split on first space
        :param last_name: last name (opt str) if not given, defaults to split on first space
        :param email: user's email (opt str)
        :param gender: self-explanatory (opt str)
        :param image_url: URL to hosted image to use as profile pic (opt str)
        :returns: None
        """
        name = name.encode('utf-8') if name else ""
        email = email.encode('utf-8') if email else ""
        first_name = first_name or name.split(" ", 1)[0]
        last_name = last_name or (name.split(" ", 1)[1] if " " in name else "")

        db.engine.execute(user_table.update().where(user_table.c.id == self.id)\
            .values(name=name or self.name, first_name=first_name or self.first_name,
                    last_name=last_name or self.last_name, email=email or self.email,
                    gender=gender or self.gender,
                    image_url=image_url or self.image_url
            )
        )
        self.name = name or self.name
        self.first_name = first_name or self.first_name
        self.last_name = last_name or self.last_name
        self.email = email or self.email
        self.gender = gender or self.gender
        self.image_url = image_url or self.image_url

    def hello(self, device_type, device_id, lang, push_on_temp=False):
        """
        Update profile information with app 'Hello' data.

        :param device_type: either 'ios' or 'android' (str)
        :param device_id: the iOS/Android unique device identifier (str)
        :param lang: ISO-639 code for user's language ('en', 'fr', etc) (str)
        :param push_on_temp: Send push notifications for temporary restrictions? (bool)
        :returns: None
        """
        now = datetime.datetime.now()
        db.engine.execute(user_table.update().where(user_table.c.id == self.id)\
            .values(device_type=device_type or None, device_id=device_id or None,
                    lang=lang or None, last_hello=now, push_on_temp=push_on_temp
            )
        )
        if device_id and device_type and device_id != self.device_id:
            if current_app.config['DEBUG'] and device_type == 'ios':
                db.redis.hset('prkng:hello-amazon:ios-sbx', str(self.id), device_id)
            else:
                db.redis.hset('prkng:hello-amazon:'+device_type, str(self.id), device_id)
        self.device_type = device_type or None
        self.device_id = device_id or None
        self.lang = lang or None
        self.last_hello = now
        self.push_on_temp = push_on_temp

    @property
    def json(self):
        """
        Return the User object as a serializable dictionary.
        (Property)

        :return: User object (dict)
        """
        vals = {
            key: value for key, value in self.__dict__.items()
        }
        # since datetime is not JSON serializable
        vals['created'] = self.created.strftime("%Y-%m-%dT%H:%M:%SZ")
        if vals.get('last_hello'):
            vals['last_hello'] = self.last_hello.strftime("%Y-%m-%dT%H:%M:%SZ")
        return vals

    @staticmethod
    def generate_apikey(email):
        """
        Generate an API key for the given user.
        The API key is a JSON Web Signature (JWS) encoded with the user's email and current timestamp.

        :param email: email address (str)
        :returns: JSON Web Signature (str)
        """
        serial = JSONWebSignatureSerializer(current_app.config['SECRET_KEY'])
        return serial.dumps({
            'email': email,
            'time': time.time()
        })

    @staticmethod
    def get(id):
        """
        Return the User for the given ID.

        :param id: user ID (int)
        :returns: User (obj) or None, as required by Flask-Login
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_all():
        """
        Obtain a list of all users as well as a few relevant properties.
        To be used for admin functions only.

        :returns: list of User objects as dicts
        """
        res = db.engine.execute("""
            SELECT DISTINCT ON (u.id) u.id, u.first_name, u.last_name, u.email, u.lang, u.device_type,
                to_char(u.created, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS created,
                to_char(u.last_hello, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS last_hello,
                x.city AS last_city, array_to_string(ae.share_svcs, ',') AS carshares,
                count(c.id) AS count
            FROM users u
            LEFT JOIN checkins c ON c.user_id = u.id
            LEFT JOIN (
                SELECT user_id, array_agg(DISTINCT substring(event from 'login_(.*)$')) AS share_svcs
                FROM analytics_event
                WHERE event LIKE '%%login_%%'
                GROUP BY user_id
            ) ae ON ae.user_id = u.id
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, city
                FROM checkins
                ORDER BY user_id, checkin_time DESC
            ) x ON x.user_id = u.id
            GROUP BY u.id, x.city, ae.share_svcs
            ORDER BY u.id
        """).fetchall()
        return [
            {key: value for key, value in row.items()}
            for row in res
        ]

    @staticmethod
    def get_byemail(email):
        """
        Return the user with the given email address.

        :param email: email address (str)
        :returns: User (obj) or None, as required by Flask-Login
        """
        if not email:
            return None
        res = user_table.select(user_table.c.email == email.lower()).execute().first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_byapikey(apikey):
        """
        Checks database to see if a user exists with given API key.

        :param apikey: API key to check with (str)
        :returns: User (obj) or None, as requred by Flask-Login
        """
        res = db.engine.execute("""
            select * from users where
            substr(apikey::text, 0, 6) = substr('{0}', 0, 6)
            AND apikey = '{0}'
            """.format(apikey)).first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_profile(id):
        """
        Return the user's profile for the given ID (not the User object).

        :param id: user ID (int)
        :returns: RowProxy object (ordereddict) or None if not exists
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return res

    @staticmethod
    def add_user(name="", first_name=None, last_name=None, email=None, gender=None, image_url=None):
        """
        Add a new user. Raise an exception if it already exists.

        :param name: user's whole name concatenated (opt str)
        :param first_name: first name (opt str) if not given, defaults to split on first space
        :param last_name: last name (opt str) if not given, defaults to split on first space
        :param email: user's email (opt str)
        :param gender: self-explanatory (opt str)
        :param image_url: URL to hosted image to use as profile pic (opt str)
        :returns: RowProxy object (ordereddict) or None if not exists
        """
        apikey = User.generate_apikey(email)
        # insert data
        db.engine.execute(user_table.insert().values(
            email=email, name=name, first_name=first_name if first_name else name.split(" ", 1)[0],
            last_name=last_name if last_name else (name.split(" ", 1)[1] if " " in name else ""),
            apikey=apikey, gender=gender, image_url=image_url))
        # retrieve new user informations
        res = user_table.select(user_table.c.email == email).execute().first()
        return User(res)


class UserAuth(object):
    """
    Represents an authentication method for a user.
    One user can have multiple authentication methods (Google + Facebook, for example).
    """
    @staticmethod
    def exists(auth_id):
        """
        Check if an authentication method exists for the given auth ID.

        :param auth_id: auth ID (str)
        :returns: bool, True if method exists
        """
        res = userauth_table.select(userauth_table.c.auth_id == auth_id).execute().first()
        return res

    @staticmethod
    def update(auth_id, birthyear):
        """
        Update the authentication method with the user's birth year.

        :param auth_id: auth ID (str)
        :param birthyear: birth year (str)
        :returns: None
        """
        db.engine.execute(userauth_table.update().where(userauth_table.c.auth_id == auth_id).values(fullprofile={'birthyear': birthyear}))

    @staticmethod
    def update_password(auth_id, password, reset_code=None):
        """
        Update the password used for this authentication method.
        With a reset code (given during 'reset password' flow), will confirm that it is correct first.

        :param auth_id: auth ID (str)
        :param password: the new password to use (str)
        :param reset_code: the reset code to authorize with (str)
        :returns: bool, True if the operation completed successfully, False if the reset code was incorrect
        """
        if reset_code:
            u = userauth_table.select(userauth_table.c.auth_id == auth_id).execute().first()
            if not u or reset_code != u["reset_code"]:
                return False
        crypt_pass = pbkdf2_sha256.encrypt(password, rounds=200, salt_size=16)
        db.engine.execute(userauth_table.update().where(userauth_table.c.auth_id == auth_id).values(password=crypt_pass, reset_code=None))
        return True

    @staticmethod
    def add_userauth(user_id=None, name=None, auth_id=None, auth_type=None,
                     email=None, fullprofile=None, password=None):
        """
        Add a user authentication method.
        Typically done on new signups or logins via new methods.

        :param user_id: user ID (int)
        :param name: user's whole name concatenated (str)
        :param auth_id: new auth ID to give this method (str)
        :param auth_type: type of authentication, 'email' 'facebook' or 'google' (str)
        :param email: user's email address (str)
        :param fullprofile: dict of random profile details to save (dict)
        :param password: the user's hashed password (str)
        :returns: None
        """
        db.engine.execute(userauth_table.insert().values(
            user_id=user_id,
            auth_id=auth_id,
            auth_type=auth_type,
            password=password,
            fullprofile=fullprofile
        ))

    @staticmethod
    def send_reset_code(auth_id, email):
        """
        Send a reset password URL to the user.
        The URL to the "change password" page is embedded with a unique reset code to authorize the action.
        The reset code is persisted to the DB for verification.

        :param auth_id: auth ID (str)
        :param email: email to send code to (str)
        :returns: None
        """
        temp_passwd = random_string()[0:6]

        c = boto.ses.connect_to_region("us-west-2",
            aws_access_key_id=current_app.config["AWS_ACCESS_KEY"],
            aws_secret_access_key=current_app.config["AWS_SECRET_KEY"])
        c.send_email(
            "noreply@prk.ng",
            "prkng - Reset password",
            "Please visit the following address to change your password. If you did not request this password change, feel free to ignore this message. \n\nhttps://api.prk.ng/resetpassword?resetCode={}&email={}\n\nThanks for using prkng!".format(temp_passwd, email.replace('@', '%40')),
            email
        )

        db.engine.execute(userauth_table.update().where(userauth_table.c.auth_id == auth_id).values(reset_code=temp_passwd))
