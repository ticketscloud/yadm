from random import randint

import pytest

from yadm import Document
from yadm import fields


class Doc(Document):
    __collection__ = 'docs'
    i = fields.IntegerField()


@pytest.fixture(scope='function')
async def docs(db):
    async with db.bulk_write(Doc) as writer:
        docs = []
        for n in range(randint(10, 20)):
            doc = Doc(i=randint(-666, 666))
            await writer.insert_one(doc)
            docs.append(doc)

    return docs


@pytest.mark.asyncio
async def test_async_for(db, docs):
    agg = db.aggregate(Doc).match(i={'$gt': 0}).project(n='$i')
    count = 0

    async for item in agg:
        assert item['n'] > 0
        count += 1

    assert count == len([d.i for d in docs if d.i > 0])
