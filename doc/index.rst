.. prkng documentation master file, created by
   sphinx-quickstart on Mon Dec 22 15:48:57 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to prkng's documentation!
=================================

Contents:

.. toctree::
   :maxdepth: 2

   CHANGELOG
   API

System requirements
===================

- postgresql 9.4
- postgis 2.1
- ogr2ogr >= 1.9.0
- osm2pgsql >= 0.87
- python 2.7
- virtualenv
- uwsgi >= 2.0.8 (installed globally, not in virtualenv, for production only)

Installation (for developpement)
================================

Creating a virtualenv
---------------------




Configuration
=============

A configuration file is needed to launch the application.
If not provided, prkng will be launched with defaults settings which are probably too
minimalist for a functionnal working environment.

Example of configuration file::

    DEBUG = False
    LOG_LEVEL = 'info'
    PG_DATABASE = 'prkng'
    PG_USERNAME = 'admin'
    PG_PASSWORD = 'admin'
    PG_PORT = '5433'
    DOWNLOAD_DIRECTORY = '/tmp'

Launch the application::

    prkng serve

Go to your browser and check `<http://localhost:5000>_`

Deployement
===========

Fabric is used to:

    - bundle the whole application and its dependencies::

        fab dist

    - deploy bundle generated to remote servers

        fab arizaro deploy

To serve application with UWSGI (production usecase), it's necessary to have a
uwsgi.ini file which look like::

    [uwsgi]
    virtualenv=/home/user/prkng_venv/
    master=true
    socket=localhost:5001
    module=prkng.wsgi:app
    processes=8
    #lazy-apps=true
    daemonize=/home/user/prkng_uwsgi.log
    need-app=true
    protocol=http
    touch-reload=/home/user/prkng-uwsgi.reload


Command Line ``prkng``
======================



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

