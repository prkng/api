Welcome to prkng documentation!
===============================

Contents:

.. toctree::
   :maxdepth: 1

   CHANGELOG
   API

System requirements
===================

- postgresql >= 9.4
- postgis >= 2.1 (with shp2pgsql command line)
- ogr2ogr >= 1.9.0
- osm2pgsql >= 0.87
- osmconvert (osmctools package)
- python >= 2.7 <3
- pip package installer >= 1.5
- virtualenv
- uwsgi >= 2.0.8 (installed globally, not in virtualenv, for production only)


Database configuration
======================

This document assumes that you have already configured a postgresql database running
with the postgis extension.

Configuration recommended for the **shared_buffer** parameter (postgresql.conf) is 1/4 of the RAM.
The **checkpoint_segment** can be increased to 128 to speed up data loading.

Developping on prkng
====================

Checkout the code
-----------------

::

    git clone https://github.com/ArnaudA/prkng-api prkng
    cd prkng
    # create an isolated python environment
    virtualenv venv
    source venv/bin/activate

Install the project and its dependencies in editable mode ::

    pip install -r requirements-dev.txt


Minimal Configuration
---------------------

A configuration file is needed to launch the application.

You can simply create the file in the root directory of the target with this name ``prkng.cfg``.
It will be used automatically.

Or you can create a file pointed with an environment variable named ``PRKNG_SETTINGS``

On Linux, you can export it via ::

    export PRKNG_SETTINGS=/path/to/prkng.cfg

Example of content ::

    DEBUG = True
    PROPAGATE_EXCEPTIONS = True
    LOG_LEVEL = 'debug'
    PG_DATABASE = 'prkng'
    PG_USERNAME = 'user'
    PG_PASSWORD = '***'
    DOWNLOAD_DIRECTORY = '/tmp'

    PG_TEST_HOST = 'localhost'
    PG_TEST_DATABASE = 'prkng_test'
    PG_TEST_PORT = '5432'
    PG_TEST_USERNAME = 'user'
    PG_TEST_PASSWORD = '***'

    ADMIN_USER = 'admin'
    ADMIN_PASS = '***'

Command Line ``prkng``
----------------------

::

    prkng update

This command will :

    - download the most recent parking informations for:

        - Montréal
        - Québec

    - download associated OpenStreetMap areas
    - load the previous data in the postgresql database (overwrite older data)
    - load districts (shapefiles provided in the repo for each city)

::

    prkng process

This command will process all data and generate parking slots (will erase any older data)


::

    prkng serve

Launch a developpement server.
Go to your browser and check `<http://localhost:5000>_`


Build this documentation
------------------------

::

    cd doc/
    sphinx-build . _build/

Go to <file:///home/user/path/to/prkng/doc/_build/>_


Launch tests
------------

::

    py.test -v


Build an archive for deployement
--------------------------------

Build an archive for prkng and its python dependencies::

    pip wheel --wheel-dir=`python setup.py --fullname` -r requirements.txt


Deploy application in production
================================

Prepare a virtual python environnement::

    cd /home/user/
    mkdir prkng && cd prkng
    virtualenv venv

Install prkng from the wheel directory (if pushed by the devvelopper)::

    pip install --force-reinstall --ignore-installed \
                --upgrade --no-index \
                --find-links=`python setup.py --fullname` prkng

Create a configuration file named prkng.cfg and fill in with the right values,
database names, credentials etc::

    export PRKNG_SETTINGS=/path/to/prkng.cfg

Repeat the next two commands to fill in the database:

    prkng update
    prkng process


Now the application is ready to serve parking slots and much more !!


The recommended stack to serve the application is:

    - uwsgi to launch the wsgi application prkng (because the Flask dev server is pretty slow).
    - nginx as a webserver that speaks natively the uwsgi protocol for the front-end.

To serve application with UWSGI, it's necessary to have a
uwsgi.ini file which look like::

    [uwsgi]
    virtualenv=/home/user/prkng/venv/
    master=true
    socket=localhost:5001
    module=prkng.wsgi:app
    processes=4
    daemonize=/home/user/prkng/prkng_uwsgi.log
    need-app=true
    touch-reload=/home/user/prkng/prkng-uwsgi.reload

to start the application, just run ::

    uwsgi --ini uwsgi.ini

to restart the application, just touch the file ``/home/user/prkng/prkng-uwsgi.reload``

Example of nginx configuration ::

    upstream prkng_api {
      server 127.0.0.1:5001;
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

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

