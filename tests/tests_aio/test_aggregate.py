from random import randint

import pytest

from yadm import Document
from yadm import fields
# from yadm.aio.aggregation import AioAggregator


class Doc(Document):
    __collection__ = 'docs'
    i = fields.IntegerField()


@pytest.fixture(scope='function')
def docs(loop, db):
    async def gen_docs():
        docs = []
        for n in range(randint(10, 20)):
            doc = Doc(i=randint(-666, 666))
            await db.insert_one(doc)
            docs.append(doc)

        return docs

    return loop.run_until_complete(gen_docs())


def test_async_for(loop, db, docs):
    async def test():
        agg = db.aggregate(Doc).match(i={'$gt': 0}).project(n='$i')
        count = 0

        async for item in agg:
            assert item['n'] > 0
            count += 1

        assert count == len([d.i for d in docs if d.i > 0])

    loop.run_until_complete(test())
