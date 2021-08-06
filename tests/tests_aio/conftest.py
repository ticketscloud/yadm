import pytest
import motor.motor_asyncio

from yadm.aio.database import AioDatabase


@pytest.fixture()
def client(mongo_args):
    host, port, _ = mongo_args
    return motor.motor_asyncio.AsyncIOMotorClient(host=host, port=port)


@pytest.fixture()
def db(event_loop, client, mongo_args):
    _, _, name = mongo_args
    event_loop.run_until_complete(client.drop_database(name))
    return AioDatabase(client, name)
