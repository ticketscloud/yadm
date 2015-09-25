from unittest import SkipTest

import pytest
import pymongo

from yadm.database import Database


@pytest.fixture(scope='session')
def client(request):
    try:
        return pymongo.MongoClient("localhost", 27017, tz_aware=True)
    except pymongo.errors.ConnectionFailure:
        raise SkipTest("Can't connect to database (localhost:27017/test)")


@pytest.yield_fixture(scope='function')
def db(client):
    client.drop_database('test')
    yield Database(client, 'test')
