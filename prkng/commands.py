# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

Command line utilities
"""
from __future__ import print_function

from prkng import create_app
from prkng.logger import Logger
from subprocess import check_call

import click
import datetime
import os


@click.group()
def main():
    pass


@click.command()
def serve():
    """
    Run a local server and serves the application
    """
    from wsgi import app
    app.run(host="0.0.0.0")


@click.command()
@click.option('--city', default='all',
    help='A specific city to fetch data for (instead of all)')
def update(city):
    """
    Update data sources
    """
    from prkng.downloader import DataSource, OsmLoader, PermitZonesLoader
    osm = OsmLoader()
    pzl = PermitZonesLoader()
    pzl.update()
    for source in DataSource.__subclasses__():
        obj = source()
        if not city == 'all' and obj.city != city:
            continue
        obj.download()
        obj.load()
        obj.load_rules()
        # download osm data related to data extent
        osm.download(obj.name, obj.get_extent())

    # load every osm files in one shot
    osm.load(city)


@click.command(name="update-areas")
def update_areas():
    """
    Create a new version of service area statics and upload to S3
    """
    from prkng.downloader import ServiceAreasLoader
    sal = ServiceAreasLoader()
    sal.process_areas()


@click.command()
def process():
    """
    Process data and create the target tables
    """
    from prkng.processing import pipeline
    pipeline.run()


@click.command()
def car2go():
    """
    Update local car2go data
    """
    from prkng.car2go import update
    update()


@click.command()
def backup():
    """
    Dump the database to file
    """
    CONFIG = create_app().config
    backup_dir = os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))), 'backup')
    file_name = 'prkng-{}.sql.gz'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)
    Logger.info('Creating backup...')
    check_call('pg_dump -c -U {PG_USERNAME} {PG_DATABASE} | gzip > {}'.format(os.path.join(backup_dir, file_name), **CONFIG),
        shell=True)
    Logger.info('Backup created and stored as {}'.format(os.path.join(backup_dir, file_name)))


@click.command()
def maintenance():
    """
    Toggle maintenance mode (set NGINX to return 503s for everything)
    """
    if not os.path.exists('/etc/nginx/sites-available/prkng'):
        Logger.error('Could not set maintenance mode.')
    if os.path.realpath('/etc/nginx/sites-enabled/prkng') == '/etc/nginx/sites-available/prkng':
        os.unlink('/etc/nginx/sites-enabled/prkng')
        os.symlink('/etc/nginx/sites-available/prkng-maint', '/etc/nginx/sites-enabled/prkng')
        Logger.info('Maintenance mode ON')
    else:
        os.unlink('/etc/nginx/sites-enabled/prkng')
        os.symlink('/etc/nginx/sites-available/prkng-maint', '/etc/nginx/sites-enabled/prkng')
        Logger.info('Maintenance mode OFF')
    check_call('service nginx reload')



main.add_command(serve)
main.add_command(update)
main.add_command(process)
main.add_command(update_areas)
main.add_command(car2go)
main.add_command(backup)
main.add_command(maintenance)
