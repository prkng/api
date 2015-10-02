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
import geojson
import gzip
import requests
import StringIO

from boto.s3.key import Key
from boto.s3.connection import S3Connection

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
        self.city = 'montreal'
        # ckan API
        self.url_signs = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show?id=stationnement-sur-rue-signalisation-courant"

        self.url_roads = "http://donnees.ville.montreal.qc.ca/api/3/action/package_show?id=geobase"

        self.resources = (
            'Ahuntsic-Cartierville',
            'Côte-des-Neiges-Notre-Dame-de-Grâce',
            'Mercier-Hochelaga-Maisonneuve',
            'Outremont',
            'Plateau-Mont-Royal',
            'Rosemont-La Petite-Patrie',
            'Saint-Laurent',
            'Le Sud-Ouest',
            'Ville-Marie',
            'Villeray-Saint-Michel-Parc-Extension',
            'signalisation-description-panneau'
        )

        self.jsonfiles = []
        self.paid_zone_shapefile = script('paid_montreal_zones.kml')

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
            "shp2pgsql -d -g geom -s 2145:3857 -W LATIN1 -I {filename} montreal_geobase | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.road_shapefile, **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "montreal_geobase")

        check_call(
            "shp2pgsql -d -g geom -s 2145:3857 -W LATIN1 -I {filename} montreal_bornes | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('bornes_montreal.shp'), **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "montreal_bornes")

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
        self.city = 'quebec'
        self.url = "http://donnees.ville.quebec.qc.ca/Handler.ashx?id=7&f=SHP"
        self.url_payant = "http://donnees.ville.quebec.qc.ca/Handler.ashx?id=8&f=SHP"

    def download(self):
        """
        Download and unzip file
        """
        Logger.info("Downloading {} parking data".format(self.name))
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

        Logger.info("Downloading {} paid parking data".format(self.name))
        zipfile = download_progress(
            self.url_payant,
            "quebec_paid_latest.zip",
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with ZipFile(zipfile) as zip:
            self.filename_payant = join(CONFIG['DOWNLOAD_DIRECTORY'], [
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

        check_call(
            "shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I "
            "{filename} quebec_bornes | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=self.filename_payant, **CONFIG),
            shell=True
        )

    def load_rules(self):
        """
        load parking rules translation
        """
        Logger.info("Loading parking rules for {}".format(self.name))

        filename = script("rules_quebec.csv")

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


class NewYork(DataSource):
    """
    Download data from New York City
    """
    def __init__(self):
        super(NewYork, self).__init__()
        self.city = 'newyork'
        # ckan API
        self.url_signs = "http://a841-dotweb01.nyc.gov/datafeeds/ParkingReg/Parking_Regulation_Shapefile.zip"
        self.url_roads = "https://data.cityofnewyork.us/api/geospatial/exjm-f27b?method=export&format=Shapefile"

    def download(self):
        self.download_signs()
        self.download_roads()

    def download_roads(self):
        """
        Download NYC Street Centerline (CSCL) shapefile
        """
        Logger.info("Downloading New York Centerlines")
        zipfile = download_progress(
            self.url_roads,
            "nyc_cscl.zip",
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
        Logger.info("Downloading New York sign data")
        zipfile = download_progress(
            self.url_signs,
            basename(self.url_signs),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        Logger.info("Unzipping")
        with ZipFile(zipfile) as zip:
            self.sign_shapefile = join(CONFIG['DOWNLOAD_DIRECTORY'], [
                name for name in zip.namelist()
                if name.lower().endswith('.shp')
            ][0])
            zip.extractall(CONFIG['DOWNLOAD_DIRECTORY'])

    def load(self):
        """
        Loads shapefiles into database
        """
        check_call(
            'shp2pgsql -d -g geom -s 4326:3857 -W LATIN1 -I {filename} newyork_sign | '
            'psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}'
            .format(filename=self.sign_shapefile, **CONFIG),
            shell=True
        )

        self.db.vacuum_analyze("public", "newyork_sign")

        check_call(
            'shp2pgsql -d -g geom -s 2263:3857 -W LATIN1 -I {filename} newyork_lines | '
            'psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}'
            .format(filename=self.road_shapefile, **CONFIG),
            shell=True
        )
        self.db.vacuum_analyze("public", "newyork_lines")

        # TODO process and import NYC street terms dictionary

    def load_rules(self):
        """
        load parking rules translation
        """
        pass

    def get_extent(self):
        """
        get extent in the format latmin, longmin, latmax, longmax
        """
        res = self.db.query(
            """WITH tmp AS (
                SELECT st_transform(st_envelope(st_collect(geom)), 4326) as geom
                FROM newyork_sign
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
        Logger.debug("https://overpass-api.de/api/interpreter?data=(way({});>;);out;"
                     .format(','.join(map(str, extent))))
        osm_file = download_progress(
            "https://overpass-api.de/api/interpreter?data=(way({});>;);out;"
            .format(','.join(map(str, extent))),
            '{}.osm'.format(name.lower()),
            CONFIG['DOWNLOAD_DIRECTORY']
        )

        self.queue.append(osm_file)

    def load(self, city):
        """
        Load data using osm2pgsql
        """
        if city == 'all':
            process_file = join(CONFIG['DOWNLOAD_DIRECTORY'], 'merged.osm')

            # merge files before loading because osm2pgsql failed to load 2 osm files
            # at the same time
            check_call("osmconvert {files} -o={merge}".format(
                files=' '.join(self.queue),
                merge=process_file),
                shell=True)
        else:
            process_file = join(CONFIG['DOWNLOAD_DIRECTORY'], self.queue[0])

        check_call(
            "osm2pgsql -E 3857 -d {PG_DATABASE} -H {PG_HOST} -U {PG_USERNAME} "
            "-P {PG_PORT} {osm_file}".format(
                osm_file=process_file,
                **CONFIG),
            shell=True
        )

        # add indexes on OSM lines
        self.db.create_index('planet_osm_line', 'way', index_type='gist')
        self.db.create_index('planet_osm_line', 'osm_id')
        self.db.create_index('planet_osm_line', 'name')
        self.db.create_index('planet_osm_line', 'highway')
        self.db.create_index('planet_osm_line', 'boundary')


class PermitZonesLoader(object):
    """
    Import permit zone shapefiles
    """
    def __init__(self):
        self.db = PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))

    def update(self):
        Logger.info("Importing permit zone shapefiles")
        check_call(
            "shp2pgsql -d -g geom -s 3857 -W LATIN1 -I "
            "{filename} permit_zones | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('permit_zones.shp'), **CONFIG),
            shell=True
        )
        self.db.create_index('permit_zones', 'geom', index_type='gist')


class ServiceAreasLoader(object):
    """
    Import service area shapefiles, upload statics to S3
    """
    def __init__(self):
        self.db = PostgresWrapper(
            "host='{PG_HOST}' port={PG_PORT} dbname={PG_DATABASE} "
            "user={PG_USERNAME} password={PG_PASSWORD} ".format(**CONFIG))
        self.bucket = S3Connection(CONFIG["AWS_ACCESS_KEY"],
            CONFIG["AWS_SECRET_KEY"]).get_bucket('prkng-service-areas')
        self.areas_qry = """
            SELECT
                gid AS id,
                name,
                name_disp,
                ST_As{}(ST_Transform(geom, 4326)) AS geom
            FROM cities
        """
        self.mask_qry = """
            SELECT
                1,
                'world_mask',
                'world_mask',
                ST_As{}(ST_Transform(geom, 4326)) AS geom
            FROM cities_mask
        """

    def upload_kml(self, version, query, gz=False):
        kml_res = self.db.query(query.format("KML"))
        kml = ('<?xml version="1.0" encoding="utf-8"?>'
            '<kml xmlns="http://www.opengis.net/kml/2.2">'
                '{}'
            '</kml>').format(''.join(['<Placemark>'+x[3]+'</Placemark>' for x in kml_res]))

        strio = StringIO.StringIO()
        kml_file = gzip.GzipFile(fileobj=strio, mode='w')
        kml_file.write(kml)
        kml_file.close()
        strio.seek(0)
        key1 = self.bucket.new_key('{}.kml.gz'.format(version))
        key1.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Type": "application/gzip"})
        strio.seek(0)
        key2 = self.bucket.new_key('{}.kml'.format(version))
        key2.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Encoding": "gzip",
            "Content-Type": "application/xml"})
        return key1.generate_url(0)

    def upload_geojson(self, version, query):
        json_res = self.db.query(query.format("GeoJSON"))
        json = geojson.dumps(geojson.FeatureCollection([
            geojson.Feature(
                id=x[0],
                geometry=geojson.loads(x[3]),
                properties={"id": x[0], "name": x[1], "name_disp": x[2]}
            ) for x in json_res
        ]))

        strio = StringIO.StringIO()
        json_file = gzip.GzipFile(fileobj=strio, mode='w')
        json_file.write(json)
        json_file.close()
        strio.seek(0)
        key1 = self.bucket.new_key('{}.geojson.gz'.format(version))
        key1.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Type": "application/gzip"})
        strio.seek(0)
        key2 = self.bucket.new_key('{}.geojson'.format(version))
        key2.set_contents_from_file(strio, {"x-amz-acl": "public-read",
            "Content-Encoding": "gzip",
            "Content-Type": "application/json"})
        return key1.generate_url(0)

    def process_areas(self):
        """
        Reload service area statics from source and upload new version of statics to S3
        """
        self.db.query("""
            CREATE TABLE IF NOT EXISTS city_assets (
                id serial PRIMARY KEY,
                version integer,
                kml_addr varchar,
                kml_mask_addr varchar,
                geojson_addr varchar,
                geojson_mask_addr varchar
            )
        """)

        version_res = self.db.query("""
            SELECT version
            FROM city_assets
            ORDER BY version DESC
            LIMIT 1
        """)
        version = str((version_res[0][0] if version_res else 0) + 1)

        Logger.info("Importing service area shapefiles")
        check_call(
            "shp2pgsql -d -g geom -s 3857 -W LATIN1 -I "
            "{filename} cities | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('service_areas.shp'), **CONFIG),
            shell=True
        )
        check_call(
            "shp2pgsql -d -g geom -s 3857 -W LATIN1 -I "
            "{filename} cities_mask | "
            "psql -q -d {PG_DATABASE} -h {PG_HOST} -U {PG_USERNAME} -p {PG_PORT}"
            .format(filename=script('service_areas_mask.shp'), **CONFIG),
            shell=True
        )
        self.db.query("""update cities
            set geom = st_makevalid(geom) where not st_isvalid(geom)""")
        self.db.create_index('cities', 'geom', index_type='gist')
        self.db.vacuum_analyze("public", "cities")

        Logger.info("Exporting new version of statics to S3")
        kml_url = self.upload_kml(version, self.areas_qry)
        kml_mask_url = self.upload_kml(version + ".mask", self.mask_qry)
        json_url = self.upload_geojson(version, self.areas_qry)
        json_mask_url = self.upload_geojson(version + ".mask", self.mask_qry)

        Logger.info("Saving metadata")
        self.db.query("""
            INSERT INTO city_assets
                (version, kml_addr, kml_mask_addr, geojson_addr, geojson_mask_addr)
            SELECT {}, '{}', '{}', '{}', '{}'
        """.format(version, kml_url.split('?')[0], kml_mask_url.split('?')[0],
            json_url.split('?')[0], json_mask_url.split('?')[0]))
