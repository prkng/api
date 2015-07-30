import os

from fabric.api import *

# use ssh env to check users, passwords and hostnames
env.use_ssh_config = True
# the servers where the commands are executed
env.hosts = ['api-test.prk.ng']


# figure out the release name and version
dist = local('python setup.py --fullname', capture=True).strip()

# where to deploy the application
env.remote_directory = '/home/prkng'


@task
def production():
    """
    set production as remote
    """
    print('***** WARNING !!! *****')
    print('You are trying to push to production. This could be a very bad idea.')
    print('Please make sure you know what you are doing!')
    print('***** *********** *****')
    prompt('Type PRKNG in all caps to continue or Ctrl-C to exit: ', validate='PRKNG')
    env.hosts = ['api.prk.ng']
    env.remote_directory = '/home/parkng'


@task
def staging():
    """
    set staging as remote
    """
    env.hosts = ['api-test.prk.ng']


@task
def archive():
    """
    create a new source distribution as tarball
    """
    local('pip wheel --wheel-dir={} -r requirements.txt'.format(dist))


@task
def deploy():
    """
    upload the archive and install the application and its dependencies
    the application
    """
    with cd(remote_directory):
        put(dist, '.')

    with cd(remote_directory), prefix('. venv/bin/activate'):
        run('pip install --force-reinstall --ignore-installed '
            '--upgrade --no-index --find-links={} prkng'.format(dist))


@task
def update():
    """
    update data sources on the server
    """
    with cd(remote_directory), prefix('. venv/bin/activate'):
        run('prkng update'.format(dist))


@task
def update_areas():
    """
    update service area sources on the server and on s3
    """
    with cd(remote_directory), prefix('. venv/bin/activate'):
        run('prkng update-areas'.format(dist))


@task
def process():
    """
    process data on the server
    """
    with cd(remote_directory), prefix('. venv/bin/activate'):
        run('prkng maintenance')
        run('killall -INT uwsgi')
        run('prkng backup')
        run('prkng process')
        run('prkng maintenance')


@task
def restart():
    """
    touch the .wsgi file to restart the application
    """
    run('touch {}'.format(os.path.join(env.remote_directory, 'prkng-uwsgi.reload')))
