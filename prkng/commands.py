# -*- coding: utf-8 -*-
"""
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
    app.run()


@click.command()
def update():
    """
    Update data sources
    """
    from prkng.downloader import DataSource, OsmLoader
    osm = OsmLoader()
    for source in DataSource.__subclasses__():
        obj = source()
        obj.download()
        obj.load()
        obj.load_rules()
        # download osm data related to data extent
        osm.download(obj.name, obj.get_extent())

    # load every osm files in one shot
    osm.load()


@click.command()
def process():
    """
    Process source data and create the target model
    """
    from prkng.processing import create_model
    create_model.main()

main.add_command(serve)
main.add_command(update)
main.add_command(process)
