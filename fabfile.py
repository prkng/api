from fabric.api import *

# use ssh env to check users, passwords and hostnames
env.use_ssh_config = True
# the servers where the commands are executed
env.hosts = ['arizaro']


# figure out the release name and version
dist = local('python setup.py --fullname', capture=True).strip()


@task
def pack():
    # create a new source distribution as tarball
    local('pip wheel --wheel-dir={} .'.format(dist))


@task
def deploy():
    # upload the source tarball to the temporary folder on the server
    with cd('/home/lde/prkng'):
        put(dist, '.')

    with cd('/home/lde/prkng'), prefix('. venv/bin/activate'):
        run('pip install --force-reinstall --ignore-installed '
            '--upgrade --no-index --find-links={} prkng'.format(dist))


@task
def restart():
    """
    Check if already started else restart the remote uwsgi process
    """
    # touch the .wsgi file so that mod_wsgi triggers
    # a reload of the application
    run('touch /home/lde/prkng/prkng-uwsgi.reload')
