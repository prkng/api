# -*- coding: utf-8 -*-
"""
:author: ludovic.delaune@oslandia.com

Command line utilities
"""
from __future__ import print_function

import click


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


main.add_command(serve)
main.add_command(update)
main.add_command(process)
main.add_command(update_areas)
main.add_command(car2go)
