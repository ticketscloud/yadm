import asyncio

import pytest
import motor.motor_asyncio

from yadm.aio.database import AioDatabase


@pytest.fixture(scope='function')
def loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope='session')
def client(request, mongo_args):
    host, port, name = mongo_args
    return motor.motor_asyncio.AsyncIOMotorClient(host=host, port=port)


@pytest.yield_fixture(scope='function')
def db(loop, client, mongo_args):
    host, port, name = mongo_args
    loop.run_until_complete(client.drop_database(name))
    yield AioDatabase(client, name)
