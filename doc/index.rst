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

Command Line ``prkng``
======================

::

    prkng update

This command will :

    - download the most recent parking informations for:

        - Montréal
        - Québec

    - download associated OpenStreetMap areas
    - load the previous things in the postgresql database (overwrite older data)


::

    prkng process

This command will process all data and generate parking slots (will erase any older data)


::

    prkng serve

Launch a developpement server.
Go to your browser and check `<http://localhost:5000>_`


Build this documentation
========================

::

    cd doc/
    sphinx-build . _build/

Go to <file:///home/user/path/to/prkng/doc/_build/>_

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

