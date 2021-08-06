import pytest


from yadm.documents import Document
from yadm.fields import IntegerField
from yadm.bulk_writer import BATCH_SIZE


class Doc(Document):
    __collection__ = 'testdocs'
    i = IntegerField()


@pytest.fixture(scope='function')
async def inserted(db):
    documents = [Doc(i=i) for i in range(10)]
    await db.insert_many(documents)
    return documents


@pytest.mark.parametrize('batch_size', [3, BATCH_SIZE])
@pytest.mark.asyncio
async def test_insert_one(db, batch_size):
    async with db.bulk_write(Doc, batch_size=batch_size) as writer:
        for doc in (Doc(i=i) for i in range(10)):
            await writer.insert_one(doc)

    assert writer.result.inserted_count == 10
    assert await db.db['testdocs'].count_documents({}) == 10


@pytest.mark.parametrize('batch_size', [3, BATCH_SIZE])
@pytest.mark.asyncio
async def test_complex(db, inserted, batch_size):
    async with db.bulk_write(Doc, batch_size=batch_size) as writer:
        for doc in (Doc(i=i) for i in range(10, 20)):
            await writer.insert_one(doc)

        await writer.update_one({'i': 6}, {'$set': {'i': 66}})
        await writer.update_many({'i': {'$gte': 18}}, {'$inc': {'i': 1}})
        await writer.replace_one({'i': 3}, Doc(i=33))
        await writer.delete_one({'i': 2})
        await writer.delete_many({'i': {'$gte': 9, '$lt': 13}})

        doc = inserted[7]
        doc.i == 77
        await writer.replace(doc)

        await writer.delete(inserted[8])

    assert writer.result.inserted_count == 10
    assert writer.result.deleted_count == 6
    assert writer.result.modified_count == 6
    assert writer.result.upserted_count == 0

    assert await db.db['testdocs'].count_documents({}) == 14
