import re
import sys

import pytest
import pymongo

from yadm.database import Database


try:
    import motor  # noqa
    WITH_MOTOR = True
except ImportError:
    WITH_MOTOR = False


def pytest_addoption(parser):
    parser.addoption(
        '--mongo',
        action='store',
        default='localhost:27017/test',
        help='MongoDB connection uri',
        metavar='"localhost:27017/test"',
    )


def pytest_ignore_collect(path, config):
    if '/tests_aio' in str(path):
        return not WITH_MOTOR or sys.version_info < (3, 6)


@pytest.fixture(scope='session')
def mongo_args(request):
    uri = request.config.getoption('--mongo')
    res = re.match(r'([\w\.]+)(?::(\d+))?(?:\/(.+))?', uri)

    if not res:
        raise ValueError(uri)

    host, port, name = res.groups()

    if not host:
        host = 'localhost'

    if not port:
        port = 27017

    if not name:
        name = 'test'

    return (host, int(port), name)


@pytest.fixture(scope='session')
def client(request, mongo_args):
    host, port, name = mongo_args
    try:
        return pymongo.MongoClient(host, port, tz_aware=True)
    except pymongo.errors.ConnectionFailure:
        raise pytest.skip("Can't connect to mongodb ({}:{})".format(host, port))


@pytest.fixture(scope='function')
def db(client, mongo_args):
    host, port, name = mongo_args
    client.drop_database(name)
    return Database(client, name)
