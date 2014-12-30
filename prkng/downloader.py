# -*- coding: utf-8 -*-
"""
Download helper for parking areas

Each downloader will :

    - download from an URL (opendata provided by cities),
    - load into database,
    - download related openstreetmap data
"""
from __future__ import print_function
from subprocess import check_call
from os.path import join, basename
from zipfile import ZipFile
import requests

from logger import Logger
from utils import download_progress
from prkng import create_app
from database import PostgresWrapper


# get global config through flask application
CONFIG = create_app().config


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
        self.url = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show"\
                   "?id=stationnement-sur-rue-signalisation-courant"
        self.subset = (
            'Plateau-Mont-Royal',
            'La Petite-Patrie',
            'Le Sud-Ouest',
            'Ville-Marie',
        )

        self.jsonfiles = []

    def download(self):
        json = requests.get(self.url).json()
        subs = {}
        for res in json['result']['resources']:
            for sub in self.subset:
                if sub in res['name']:
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
                self.jsonfiles.append(join(CONFIG['DOWNLOAD_DIRECTORY'], [
                    name for name in zip.namelist()
                    if name.lower().endswith('.json')
                ][0]))
                zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def load(self):
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

        # echo 'Loading description of panneaux' &&
        # psql -d prkng -f load_descr.sql

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

        self.db.vacuum_analyze("public", "quebec_panneau")

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


def osm_url(bbox):
    """
    Helper to download openstreetmap data using the overpass api

    :param tuple bbox: bounding box: lat min, long min, lat max, long max
    :returns: file created
    """
    url = "http://overpass.osm.rambler.ru/cgi/interpreter?data=(way({}));out;".format(
          ','.join(map(str, bbox))),
    Logger.debug(url)
    return url


class OsmLoader(object):
    """
    Load osm data according to bbox given
    """
    def __init__(self):
        # queue containing osm filenames
        self.queue = []

    def download(self, name, extent):
        Logger.info("Getting Openstreetmap ways for {}".format(name))
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
        check_call(
            "osm2pgsql -E 3857 -d {PG_DATABASE} -H {PG_HOST} -U {PG_USERNAME} -P {PG_PORT} {osm_files}"
            .format(osm_files=' '.join(self.queue), **CONFIG),
            shell=True
        )
