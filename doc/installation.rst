Installing
##########

System requirements
===================

- postgresql >= 9.4
- postgresql-server-dev-9.4
- postgresql-contrib-9.4
- postgis >= 2.1 (with shp2pgsql command line)
- ogr2ogr >= 1.9.0 (gdal-bin package)
- osm2pgsql >= 0.87
- osmconvert (osmctools package)
- python >= 2.7 <3
- python-dev >= 2.7 <3
- pip package installer >= 1.5
- virtualenv
- uwsgi >= 2.0.8 (installed globally, not in a virtualenv, for production only)
- git
- nodejs >= 0.10.35


Database configuration
======================

This document assumes that you have already configured a postgresql database running
with the postgis extension.

Configuration recommended for the **shared_buffer** parameter (postgresql.conf) is 1/4 of the RAM.
The **checkpoint_segment** can be increased to 128 to speed up data loading.


Quick summary to create a new database

.. code-block:: bash

    $ su postgres
    $ psql
    postgres=# create user prkng with password 'prkng' superuser;
    postgres=# create database prkng owner prkng encoding 'utf-8';
    postgres=# \c prkng prkng
    Password for user prkng: ****
    You are now connected to database "prkng" as user "prkng"
    prkng=> create extension postgis;

Remember to change the database name if you are running a test instance (i.e. `prkng_test`).


Development mode
==================

Checkout the code
-----------------

.. code-block:: bash

    $ git clone https://github.com/ArnaudA/prkng-api prkng
    $ cd prkng
    # create an isolated python environment
    $ virtualenv venv
    $ source venv/bin/activate

Install the project and its dependencies in editable mode

.. code-block:: bash

    $ pip install -r requirements-dev.txt


Minimal Configuration
---------------------

A configuration file is needed to launch the application.

You can simply create the file in the root directory of the project with this name ``prkng.cfg``.
It will be used automatically.

Or you can create a file pointed with an environment variable named ``PRKNG_SETTINGS``

On Linux, you can export it via

.. code-block:: bash

    $ export PRKNG_SETTINGS=/path/to/prkng.cfg

Example of content ::

    DEBUG = True
    PROPAGATE_EXCEPTIONS = True
    LOG_LEVEL = 'debug'
    PG_DATABASE = 'prkng'
    PG_USERNAME = 'user'
    PG_PASSWORD = '***'
    DOWNLOAD_DIRECTORY = '/tmp'  # used to download temporary data

    PG_TEST_HOST = 'localhost'
    PG_TEST_DATABASE = 'prkng_test'
    PG_TEST_PORT = '5432'
    PG_TEST_USERNAME = 'user'
    PG_TEST_PASSWORD = '***'

    # for the admin site
    ADMIN_USER = 'admin'
    ADMIN_PASS = '***'

    AWS_ACCESS_KEY = '***'
    AWS_SECRET_KEY = '***'
    AWS_S3_BUCKET = 'prkng-pictures'

    OAUTH_CREDENTIALS = {
        "google": {
            "id": "***",
            "secret": "***"
        },
        "facebook": {
            "id": "***",
            "secret": "***"
        }
    }



Build the documentation
-----------------------

.. code-block:: bash

    $ cd doc/
    $ make html

Go to ``<file:///home/user/path/to/prkng/doc/_build/html>`_


Build the admin interface
-------------------------

.. code-block:: bash

    $ cd prkng-admin
    $ sudo npm install -g ember-cli
    $ npm install
    $ bower install
    $ ember build


``prkng serve`` will serve this interface internally for development purposes without having it run through NGINX. You will however need to run ``ember build`` in the prkng-admin folder after you pull from Git for the interface to be updated.


Launch the tests
----------------

In order to launch the tests, you will have to create a test database in PostgreSQL
and fill the connection parameters in the ``prkng.cfg`` file

Then launching the test from the root directory

.. code-block:: bash

    $ py.test -v prkng


Command line ``prkng``
----------------------

.. code-block:: bash

    $ prkng update

This command will:

    - download the most recent parking informations for:

        - Montréal
        - Québec

    - download associated OpenStreetMap areas
    - load the previous data in the PostgreSQL database (overwrite older data)
    - load districts (shapefiles provided in the repo for each city)

.. code-block:: bash

    $ prkng process

This command will process all data and generate parking slots (will erase any older data)

.. code-block:: bash

    $ prkng serve

Launch a development server.
Go to your browser and check `<http://localhost:5000>`_


Production mode
===============

The recommended stack to serve the application is ``prkng -> uWSGI -> Nginx``

1. Get the code

.. code-block:: bash

    $ git clone https://github.com/ArnaudA/prkng-api prkng
    $ cd prkng
    # checkout the release you want
    $ git checkout v1.0.3

    # create an isolated python environment
    $ virtualenv venv
    $ source venv/bin/activate

Install the project and its dependencies inside the virtual environment

.. code-block:: bash

    $ pip install -r requirements.txt


2. Create the configuration file as explained above

Be aware to set ``DEBUG=False`` and ``LOG_LEVEL='info'``

3. Configure uWSGI

Create an empty file that just need to be touched to restart the application

    $ touch /home/parkng/prkng-uwsgi.reload

Add a uWSGI configuration file /home/parkng/prkng.uwsgi ::

    [uwsgi]
    virtualenv=/home/parkng/parkng
    master=true
    socket=/tmp/uwsgi.socket
    module=prkng.wsgi:app
    processes=3
    daemonize=/home/parkng/prkng-uwsgi.log
    need-app=true
    touch-reload=/home/parkng/prkng-uwsgi.reload

Launch the application ::

    $ uwsgi --ini prkng.uwsgi

4. Build the admin interface

    $ cd prkng-admin
    $ npm install
    $ bower install
    $ ember build

5. Nginx (which has a native support of the uWSGI protocol)

.. code-block:: bash

    $ sudo vi /etc/nginx/sites-available/prkng

::

    upstream prkng_api {
      server unix:/tmp/uwsgi.socket;
    }

    server {

        root /usr/share/nginx/www;
        index index.html index.htm;

        # Make site accessible from http://localhost/
        server_name localhost;

        location / {
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-NginX-Proxy true;
            proxy_redirect off;
            include uwsgi_params;
            uwsgi_pass prkng_api;
        }
    }

.. code-block:: bash

    $ sudo ln -s /etc/nginx/sites-available/prkng /etc/nginx/sites-enabled/
    $ sudo service nginx restart
