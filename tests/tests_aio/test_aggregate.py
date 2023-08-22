from random import randint

import pytest
import pytest_asyncio

from yadm import Document
from yadm import fields


class Doc(Document):
    __collection__ = 'docs'
    i = fields.IntegerField()


@pytest_asyncio.fixture(scope='function')
async def docs2(db):
    async with db.bulk_write(Doc) as writer:
        docs = []
        for n in range(randint(10, 20)):
            doc = Doc(i=randint(-666, 666))
            await writer.insert_one(doc)
            docs.append(doc)

    return docs


@pytest.mark.asyncio
async def test_async_for(event_loop, db, docs2):
    agg = db.aggregate(Doc).match(i={'$gt': 0}).project(n='$i')
    agg = agg.comment('qwerty')
    count = 0

    async for item in agg:
        assert item['n'] > 0
        count += 1

    assert count == len([d.i for d in docs2 if d.i > 0])
