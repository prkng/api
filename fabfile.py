from fabric.api import *

# use ssh env to check users, passwords and hostnames
env.use_ssh_config = True
# the servers where the commands are executed
env.hosts = ['arizaro']


# figure out the release name and version
dist = local('python setup.py --fullname', capture=True).strip()

# where to deploy the application
remote_directory = '/home/lde/prkng'


@task
def archive():
    """
    create a new source distribution as tarball
    """
    local('pip wheel --wheel-dir={} .'.format(dist))


@task
def deploy():
    """
    upload the source tarball to the temporary folder on the server
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
def process():
    """
    process data on the server
    """
    with cd(remote_directory), prefix('. venv/bin/activate'):
        run('prkng process'.format(dist))


@task
def restart():
    """
    touch the .wsgi file to restart the application
    """
    run('touch /home/lde/prkng/prkng-uwsgi.reload')
