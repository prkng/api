# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

Download helper for parking areas

Each downloader will :

    - download from an URL (opendata provided by cities),
    - load into database,
    - download related openstreetmap data
"""
from __future__ import print_function, unicode_literals

from subprocess import check_call
from os.path import join, basename, dirname
from zipfile import ZipFile
import requests

from logger import Logger
from utils import download_progress
from prkng import create_app
from database import PostgresWrapper


# get global config through flask application
CONFIG = create_app().config


def script(src):
    """returns the location of sql scripts"""
    return join(dirname(__file__), 'data', src)


class DataSource(object):
    """
    Base class for datasource
    """
    def __init__(self):
        self.name = self.__class__.__name__
        self.db = PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))


class Montreal(DataSource):
    """
    Download data from Montreal city
    """
    def __init__(self):
        super(Montreal, self).__init__()
        # ckan API
        self.url_signs = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show?id=stationnement-sur-rue-signalisation-courant"

        self.url_roads = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show?id=geobase"

        self.resources = (
            'Ahuntsic-Cartierville',
            'Côte-des-Neiges-Notre-Dame-de-Grâce',
            'Rosemont-La Petite-Patrie',
            'Outremont',
            'Plateau-Mont-Royal',
            'Saint-Laurent',
            'Le Sud-Ouest',
            'Ville-Marie',
            'Verdun',
            'signalisation-description-panneau',
        )

        self.jsonfiles = []

    def download(self):
        self.download_signs()
        self.download_roads()

    def download_roads(self):
        """
        Download roads (geobase) using CKAN API
        """
        json = requests.get(self.url_roads).json()
        url = ''

        for res in json['result']['resources']:
            if res['name'].lower() == 'géobase' and res['format'] == 'ZIP':
                url = res['url']

        Logger.info("Downloading Montreal Géobase")
        zipfile = download_progress(
            url.replace('ckanprod', 'donnees.ville.montreal.qc.ca'),
            basename(url),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with ZipFile(zipfile) as zip:
            self.road_shapefile = join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def download_signs(self):
        """
        Download signs using CKAN API
        """
        json = requests.get(self.url_signs).json()
        subs = {}
        for res in json['result']['resources']:
            for sub in self.resources:
                if sub.lower() in res['name'].lower():
                    subs[res['name']] = res['url']

        for area, url in subs.iteritems():
            Logger.info("Downloading Montreal - {} ".format(area))
            zipfile = download_progress(
                url.replace('ckanprod', 'donnees.ville.montreal.qc.ca'),
                basename(url),
                CONFIG['DOWNLOAD_DIRECTORY']
            )

            Logger.info("Unzipping")
            with ZipFile(zipfile) as zip:
                if 'description' not in zipfile:
                    self.jsonfiles.append(join(CONFIG['DOWNLOAD_DIRECTORY'], [
                        name for name in zip.namelist()
                        if name.lower().endswith('.json')
                    ][0]))
                else:
                    self.csvfile = join(CONFIG['DOWNLOAD_DIRECTORY'], [
                        name for name in zip.namelist()
                        if name.lower().endswith('.csv')
                    ][0])

                zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def load(self):
        """
        Loads geojson files
        """
        check_call(
            'ogr2ogr -f "PostgreSQL" PG:"dbname=prkng user={PG_USERNAME}  '
            'password={PG_PASSWORD} port={PG_PORT} host={PG_HOST}" -overwrite '
            '-nlt point -s_srs EPSG:2145 -t_srs EPSG:3857 -lco GEOMETRY_NAME=geom  '
            '-nln montreal_poteaux {}'.format(self.jsonfiles[0], **CONFIG),
            shell=True
        )
        for jsondata in self.jsonfiles[1:]:
            check_call(
                'ogr2ogr -f "PostgreSQL" PG:"dbname=prkng user={PG_USERNAME}  '
                'password={PG_PASSWORD} port={PG_PORT} host={PG_HOST}" '
                '-append -nlt point -s_srs EPSG:2145 -t_srs EPSG:3857 '
                '-nln montreal_poteaux {}'.format(jsondata, **CONFIG),
                shell=True
            )

        self.db.vacuum_analyze("public", "montreal_poteaux")

        check_call(
            "shp2pgsql -d -g geom -s 2145:3857 -W LATIN1 -I "
            "{filename} montreal_geobase | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.road_shapefile, **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "montreal_geobase")

        Logger.debug("Loading Montreal districts")
        check_call(
            "shp2pgsql -d -g geom -s 2145:3857 -W LATIN1 -I "
            "{filename} montreal_district | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('montreal_district.shp'), **CONFIG),
            shell=True
        )
        self.db.query("""update montreal_district
            set geom = st_makevalid(geom) where not st_isvalid(geom)""")
        self.db.create_index('montreal_district', 'geom', index_type='gist')
        self.db.vacuum_analyze("public", "montreal_district")

        # loading csv data using script
        Logger.debug("loading file '%s' with script '%s'" %
                     (self.csvfile, script('montreal_load_panneau_descr.sql')))

        with open(script('montreal_load_panneau_descr.sql'), 'rb') as infile:
            self.db.query(infile.read().format(description_panneau=self.csvfile))
            self.db.vacuum_analyze("public", "montreal_descr_panneau")

    def load_rules(self):
        """
        load parking rules translation
        """
        filename = script("rules_montreal.csv")

        Logger.info("Loading parking rules for {}".format(self.name))
        Logger.debug("loading file '%s' with script '%s'" %
                     (filename, script('montreal_load_rules.sql')))

        with open(script('montreal_load_rules.sql'), 'rb') as infile:
            self.db.query(infile.read().format(filename))
            self.db.vacuum_analyze("public", "montreal_rules_translation")

    def get_extent(self):
        """
        get extent in the format latmin, longmin, latmax, longmax
        """
        res = self.db.query(
            """WITH tmp AS (
                SELECT st_transform(st_envelope(st_collect(geom)), 4326) as geom
                FROM montreal_poteaux
            ) select st_ymin(geom), st_xmin(geom), st_ymax(geom), st_xmax(geom) from tmp
            """)[0]
        return res


class Quebec(DataSource):
    """
    Download data from Quebec city
    """
    def __init__(self):
        super(Quebec, self).__init__()
        self.url = "http://donnees.ville.quebec.qc.ca/Handler.ashx?id=7&f=SHP"

    def download(self):
        """
        Download and unzip file
        """
        Logger.info("Downloading {} data".format(self.name))
        zipfile = download_progress(
            self.url,
            "quebec_latest.zip",
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with ZipFile(zipfile) as zip:
            self.filename = join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def load(self):
        Logger.info("Loading {} data".format(self.name))

        check_call(
            "shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I "
            "{filename} quebec_panneau | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.filename, **CONFIG),
            shell=True
        )

        self.db.create_index('quebec_panneau', 'type_desc')
        self.db.create_index('quebec_panneau', 'nom_topog')
        self.db.create_index('quebec_panneau', 'id_voie_pu')
        self.db.create_index('quebec_panneau', 'lect_met')
        self.db.vacuum_analyze("public", "quebec_panneau")

        Logger.debug("Loading Québec districts")

        check_call(
            "shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I "
            "{filename} quebec_district | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('quebec_district.shp'), **CONFIG),
            shell=True
        )
        self.db.query("""update quebec_district
            set geom = st_makevalid(geom) where not st_isvalid(geom)""")
        self.db.create_index('quebec_district', 'geom', index_type='gist')
        self.db.vacuum_analyze("public", "quebec_district")

    def load_rules(self):
        """
        load parking rules translation
        """
        Logger.info("Loading parking rules for {}".format(self.name))

        filename = script("rules_quebec.csv")

        Logger.info("Loading parking rules for {}".format(self.name))
        Logger.debug("loading file '%s' with script '%s'" %
                     (filename, script('quebec_load_rules.sql')))

        with open(script('quebec_load_rules.sql'), 'rb') as infile:
            self.db.query(infile.read().format(filename))
            self.db.vacuum_analyze("public", "quebec_rules_translation")

    def get_extent(self):
        """
        get extent in the format latmin, longmin, latmax, longmax
        """
        res = self.db.query(
            """WITH tmp AS (
                SELECT st_transform(st_envelope(st_collect(geom)), 4326) as geom
                FROM quebec_panneau
            ) select st_ymin(geom), st_xmin(geom), st_ymax(geom), st_xmax(geom) from tmp
            """)[0]
        return res


class OsmLoader(object):
    """
    Load osm data according to bbox given
    """
    def __init__(self):
        # queue containing osm filenames
        self.queue = []
        self.db = PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    def download(self, name, extent):
        Logger.info("Getting Openstreetmap ways for {}".format(name))
        Logger.debug("overpass.osm.rambler.ru/cgi/interpreter?data=(way({});>;);out;"
                     .format(','.join(map(str, extent))))
        osm_file = download_progress(
            "http://overpass.osm.rambler.ru/cgi/interpreter?data=(way({});>;);out;"
            .format(','.join(map(str, extent))),
            '{}.osm'.format(name),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        self.queue.append(osm_file)

    def load(self):
        """
        Load data using osm2pgsql
        """
        merged_file = join(CONFIG['DOWNLOAD_DIRECTORY'], 'merged.osm')

        # merge files before loading because osm2pgsql failed to load 2 osm files
        # at the same time
        check_call("osmconvert {files} -o={merge}".format(
            files=' '.join(self.queue),
            merge=merged_file),
            shell=True)

        check_call(
            "osm2pgsql -E 3857 -d {PG_DATABASE} -H {PG_HOST} -U {PG_USERNAME} "
            "-P {PG_PORT} {osm_file}".format(
                osm_file=merged_file,
                **CONFIG),
            shell=True
        )

        # add indexes on OSM lines
        self.db.create_index('planet_osm_line', 'way', index_type='gist')
        self.db.create_index('planet_osm_line', 'osm_id')
        self.db.create_index('planet_osm_line', 'name')
        self.db.create_index('planet_osm_line', 'highway')
        self.db.create_index('planet_osm_line', 'boundary')
