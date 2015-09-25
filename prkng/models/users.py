from prkng.database import db, metadata
from prkng.utils import random_string

import boto.ses
import datetime
from flask import current_app
from flask.ext.login import UserMixin
from itsdangerous import JSONWebSignatureSerializer
from passlib.hash import pbkdf2_sha256
from sqlalchemy import Column, DateTime, ForeignKey, func, Index, Integer, String, Table, text
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
    Column('gender', String(10)),
    Column('email', String(60), index=True, unique=True, nullable=False),
    Column('created', DateTime, server_default=text('NOW()'), index=True),
    Column('device_type', String, nullable=True),
    Column('device_id', String, nullable=True),
    Column('lang', String, nullable=True),
    Column('last_hello', DateTime, server_default=text('NOW()'), nullable=True),
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
        Update key in the database
        """
        db.engine.execute("""
            update users set apikey = '{key}'
            where id = {user_id}
            """.format(key=newkey, user_id=self.id))
        self.apikey = newkey

    def update_profile(self, name=None, email=None, gender=None, image_url=None):
        """
        Update profile information
        """
        db.engine.execute(user_table.update().where(user_table.c.id == self.id)\
            .values(name=(name.encode('utf-8') if name else self.name),
                    email=(email.encode('utf-8') if email else self.email),
                    gender=gender or self.gender,
                    image_url=image_url or self.image_url
            )
        )
        self.name = name.encode('utf-8') if name else self.name
        self.email = email.encode('utf-8') if email else self.email
        self.gender = gender or self.gender
        self.image_url = image_url or self.image_url

    def hello(self, device_type, device_id, lang):
        """
        Update profile information with app hello data
        """
        now = datetime.datetime.now()
        db.engine.execute(user_table.update().where(user_table.c.id == self.id)\
            .values(device_type=device_type or None, device_id=device_id or None,
                    lang=lang or None, last_hello=now
            )
        )
        self.device_type = device_type or None
        self.device_id = device_id or None
        self.lang = lang or None
        self.last_hello = now

    @property
    def json(self):
        vals = {
            key: value for key, value in self.__dict__.items()
        }
        # since datetime is not JSON serializable
        vals['created'] = self.created.strftime("%Y-%m-%dT%H:%M:%SZ")
        vals['last_hello'] = self.last_hello.strftime("%Y-%m-%dT%H:%M:%SZ")
        return vals

    @staticmethod
    def generate_apikey(email):
        """
        Generate a user API key
        """
        serial = JSONWebSignatureSerializer(current_app.config['SECRET_KEY'])
        return serial.dumps({
            'email': email,
            'time': time.time()
        })

    @staticmethod
    def get(id):
        """
        Static method to search the database and see if user with ``id`` exists.  If it
        does exist then return a User Object.  If not then return None as
        required by Flask-Login.
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return User(res)

    @staticmethod
    def get_byemail(email):
        """
        Static method to search the database and see if user with ``id`` exists.  If it
        does exist then return a User Object.  If not then return None as
        required by Flask-Login.
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
        Static method to search the database and see if user with ``apikey`` exists.  If it
        does exist then return a User Object.  If not then return None as
        required by Flask-Login.
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
        Static method to search the database and get a user profile.
        :returns: RowProxy object (ordereddict) or None if not exists
        """
        res = user_table.select(user_table.c.id == id).execute().first()
        if not res:
            return None
        return res

    @staticmethod
    def add_user(name=None, email=None, gender=None, image_url=None):
        """
        Add a new user.
        Raise an exception in case of already exists.
        """
        apikey = User.generate_apikey(email)
        # insert data
        db.engine.execute(user_table.insert().values(
            name=name, email=email, apikey=apikey, gender=gender, image_url=image_url))
        # retrieve new user informations
        res = user_table.select(user_table.c.email == email).execute().first()
        return User(res)


class UserAuth(object):
    """
    Represent an authentication method per user.
    On user can have several authentication methods (google + facebook for example).
    """
    @staticmethod
    def exists(auth_id):
        res = userauth_table.select(userauth_table.c.auth_id == auth_id).execute().first()
        return res

    @staticmethod
    def update(auth_id, birthyear):
        db.engine.execute(userauth_table.update().where(userauth_table.c.auth_id == auth_id).values(fullprofile={'birthyear': birthyear}))

    @staticmethod
    def update_password(auth_id, password, reset_code=None):
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
        db.engine.execute(userauth_table.insert().values(
            user_id=user_id,
            auth_id=auth_id,
            auth_type=auth_type,
            password=password,
            fullprofile=fullprofile
        ))

    @staticmethod
    def send_reset_code(auth_id, email):
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
