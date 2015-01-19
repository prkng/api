import os
import re
from setuptools import setup, find_packages

PROJECT_NAME = "prkng"

here = os.path.abspath(os.path.dirname(__file__))

requirements = (
    'flask==0.10.1',
    'flask-restplus==0.4.0',
    'psycopg2==2.5.4',
    'click==3.3',
    'requests==2.5.1',
    'geojson==1.0.9',
    'aniso8601==0.92'
)


def find_version(*file_paths):
    """
    see https://github.com/pypa/sampleproject/blob/master/setup.py
    """
    with open(os.path.join(here, *file_paths), 'r') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string. "
                       "Should be at the first line of __init__.py.")

setup(
    name=PROJECT_NAME,
    version=find_version('prkng', '__init__.py'),
    description="prkng API",
    url='http://prkng.com/api',
    author='Oslandia',
    author_email='infos@oslandia.com',
    license='',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2.7',
        'License :: Other/Proprietary License'
    ],
    packages=find_packages(),
    package_data={'prkng': ['data/*']},
    install_requires=requirements,
    entry_points={
        'console_scripts': ['prkng = prkng.commands:main'],
    }
)
