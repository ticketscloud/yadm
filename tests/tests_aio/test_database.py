import random

import pytest
from bson import ObjectId

from yadm import fields
from yadm.documents import Document
from yadm.log_items import Save, Insert
from yadm.serialize import from_mongo, to_mongo
from yadm.testing import create_fake


class Doc(Document):
    __collection__ = 'testdocs'
    b = fields.BooleanField()
    i = fields.IntegerField()
    l = fields.ListField(fields.IntegerField())  # noqa: E741


@pytest.mark.asyncio
async def test_estimated_document_count(db):
    col = db.db['testdocs']
    ids = []
    for i in range(10):
        ids.append((await col.insert_one({'i': i})).inserted_id)

    count = await db.estimated_document_count(Doc)
    assert count == len(ids) == 10


@pytest.mark.asyncio
async def test_insert_one(db):
    doc = Doc()
    doc.i = 13

    await db.insert_one(doc)

    assert await db.db['testdocs'].count_documents({}) == 1
    assert (await db.db['testdocs'].find_one())['i'] == 13
    assert isinstance(doc.id, ObjectId)
    assert Insert(id=doc.id) in doc.__log__
    assert doc.__db__ is db


@pytest.mark.asyncio
async def test_insert_many(db):
    documents = [Doc(i=i) for i in range(10)]
    random.shuffle(documents)

    result = await db.insert_many(documents)

    for _id, doc in zip(result.inserted_ids, documents):
        assert doc.id == _id

    assert len(result.inserted_ids) == len(documents)


@pytest.mark.asyncio
async def test_insert_many__empty(db):
    result = await db.insert_many([])
    assert len(result.inserted_ids) == 0


@pytest.mark.asyncio
async def test_insert_many__unordered(db):
    documents = [Doc(i=i) for i in range(10)]
    random.shuffle(documents)

    result = await db.insert_many(documents, ordered=False)

    for doc in documents:
        assert not hasattr(doc, 'id')

    assert len(result.inserted_ids) == len(documents)


@pytest.mark.asyncio
async def test_save_new(db):
    doc = Doc()
    doc.i = 13

    await db.save(doc)

    assert await db.db['testdocs'].count_documents({}) == 1
    assert (await db.db['testdocs'].find_one())['i'] == 13
    assert Save(id=doc.id) in doc.__log__
    assert doc.__db__ is db


@pytest.mark.asyncio
async def test_save(db):
    doc = Doc()
    doc.i = 13
    await db.save(doc)

    doc.i = 14
    await db.save(doc)

    assert await db.db['testdocs'].count_documents({}) == 1
    assert (await db.db['testdocs'].find_one())['i'] == 14
    assert Save(id=doc.id) in doc.__log__
    assert doc.__db__ is db


@pytest.fixture()
def doc(event_loop, db):
    async def fixture():
        doc = Doc(b=True, i=13, l=[1, 2, 3])
        await db.insert_one(doc)
        return doc

    return event_loop.run_until_complete(fixture())


@pytest.mark.parametrize('kwargs, result', [
    (
        {'set': {'i': 88}},
        {'b': True, 'i': 88, 'l': [1, 2, 3]},
    ),
    (
        {'unset': {'i': True}},
        {'b': True, 'l': [1, 2, 3]},
    ),
    (
        {'unset': ['b']},
        {'i': 13, 'l': [1, 2, 3]},
    ),
    (
        {'push': {'l': 33}},
        {'b': True, 'i': 13, 'l': [1, 2, 3, 33]},
    ),
    (
        {'pull': {'l': 2}},
        {'b': True, 'i': 13, 'l': [1, 3]},
    ),
    (
        {'inc': {'i': 653}},
        {'b': True, 'i': 666, 'l': [1, 2, 3]},
    ),
])
@pytest.mark.asyncio
async def test_update_one(db, doc, kwargs, result):
    await db.update_one(doc, **kwargs)
    raw_doc = await db.db['testdocs'].find_one(doc.id)

    assert raw_doc
    del raw_doc['_id']
    assert raw_doc == result


@pytest.mark.asyncio
async def test_update_one__empty_query(db, doc):
    assert (await db.update_one(doc)) is None
    assert (await db.db['testdocs'].find_one(doc.id)) == to_mongo(doc)


@pytest.mark.asyncio
async def test_delete_one(db):
    col = db.db['testdocs']
    await col.insert_one({'i': 13})

    doc = from_mongo(Doc, await col.find_one({'i': 13}))

    assert await col.count_documents({}) == 1
    await db.delete_one(doc)
    assert await col.count_documents({}) == 0


@pytest.mark.asyncio
async def test_reload(db):
    doc = create_fake(Doc)
    b = doc.b
    await db.insert_one(doc)

    await db.db['testdocs'].update_one({'_id': doc.id},
                                       {'$set': {'b': not doc.b}})

    assert doc.b == b
    new_doc = await db.reload(doc)
    assert new_doc is doc
    assert doc.b != b
    assert doc.i


@pytest.mark.asyncio
async def test_reload__projection(db):
    doc = create_fake(Doc)
    b = doc.b
    await db.insert_one(doc)

    await db.db['testdocs'].update_one({'_id': doc.id},
                                       {'$set': {'b': not doc.b}})

    assert doc.b == b
    new_doc = await db.reload(doc, projection={'b': True})
    assert new_doc is doc
    assert doc.b != b
    assert 'i' in doc.__not_loaded__


@pytest.mark.asyncio
async def test_reload__new_instance(db):
    doc = create_fake(Doc)
    b = doc.b
    await db.insert_one(doc)

    await db.db['testdocs'].update_one({'_id': doc.id},
                                       {'$set': {'b': not doc.b}})

    assert doc.b == b
    new_doc = await db.reload(doc, new_instance=True)
    assert new_doc is not doc
    assert doc.b == b
    assert new_doc.b != b


@pytest.mark.asyncio
async def test_get_document(db):
    documents = [Doc(i=i) for i in range(5)]
    result = await db.db['testdocs'].insert_many((to_mongo(d) for d in documents))
    assert len(result.inserted_ids) == len(documents)
    for _id in result.inserted_ids:
        doc = await db.get_document(Doc, _id)
        assert doc.id == _id


@pytest.mark.asyncio
async def test_get_document__projection(db):
    documents = [Doc(i=i) for i in range(5)]
    result = await db.db['testdocs'].insert_many((to_mongo(d) for d in documents))
    assert len(result.inserted_ids) == len(documents)
    for _id in result.inserted_ids:
        doc = await db.get_document(Doc, _id, projection={'i': False})
        assert doc.id == _id
        assert 'i' in doc.__not_loaded__


@pytest.mark.asyncio
async def test_get_document__not_found(db):
    doc = await db.get_document(Doc, ObjectId())
    assert doc is None


@pytest.mark.asyncio
async def test_get_document__exc(db):
    class NotFound(Exception):
        pass

    with pytest.raises(NotFound):
        await db.get_document(Doc, ObjectId(), exc=NotFound)
