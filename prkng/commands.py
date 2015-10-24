# -*- coding: utf-8 -*-
from __future__ import print_function

from prkng import create_app
from prkng.logger import Logger
from subprocess import check_call
from prkng.tasks import init_tasks

import click
import datetime
import os
import subprocess


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
def backup():
    """
    Dump the database to file
    """
    CONFIG = create_app().config
    Logger.info('Creating backup...')
    backup_dir = os.path.join(os.path.dirname(os.environ["PRKNG_SETTINGS"]), 'backup')
    file_name = 'prkng-{}.sql.gz'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)
    subprocess.check_call('pg_dump -c -U {PG_USERNAME} {PG_DATABASE} | gzip > {}'.format(
        os.path.join(backup_dir, file_name), PG_USERNAME=CONFIG["PG_USERNAME"], PG_DATABASE=CONFIG["PG_DATABASE"]),
        shell=True)
    Logger.info('Backup created and stored as {}'.format(os.path.join(backup_dir, file_name)))


@click.command(name="import")
@click.argument('path', type=click.Path(exists=True, dir_okay=False, readable=True, resolve_path=True))
def file_import(path):
    """
    Import database from specified file location
    """
    CONFIG = create_app().config
    Logger.info('Importing backup...')
    if path.endswith(".gz"):
        cmdstring = 'gunzip -c {} | psql {PG_USERNAME} {PG_DATABASE}'
    else:
        cmdstring = 'psql {PG_USERNAME} {PG_DATABASE} < {}'
    subprocess.check_call(cmdstring.format(path, PG_USERNAME=CONFIG["PG_USERNAME"], PG_DATABASE=CONFIG["PG_DATABASE"]),
        shell=True)
    Logger.info('Data imported successfully')


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


@click.command(name="init-tasks")
def initialize_tasks():
    """
    Tell rq-scheduler to process our tasks
    """
    CONFIG = create_app().config
    init_tasks(CONFIG["DEBUG"])
    Logger.info('Tasks initialized')


main.add_command(serve)
main.add_command(backup)
main.add_command(file_import)
main.add_command(maintenance)
main.add_command(initialize_tasks)
