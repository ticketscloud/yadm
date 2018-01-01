import pytest
from bson import ObjectId

from yadm import fields
from yadm.documents import Document
from yadm.serialize import from_mongo


class Doc(Document):
    __collection__ = 'testdocs'
    b = fields.BooleanField()
    i = fields.IntegerField()
    l = fields.ListField(fields.IntegerField())


def test_insert(loop, db):
    async def test():
        doc = Doc()
        doc.i = 13

        await db.insert(doc)

        assert await db.db.testdocs.find().count() == 1
        assert (await db.db.testdocs.find_one())['i'] == 13
        assert isinstance(doc.id, ObjectId)
        assert not doc.__changed__
        assert doc.__db__ is db

    loop.run_until_complete(test())


def test_save_new(loop, db):
    async def test():
        doc = Doc()
        doc.i = 13

        await db.save(doc)

        assert await db.db.testdocs.find().count() == 1
        assert (await db.db.testdocs.find_one())['i'] == 13
        assert isinstance(doc.id, ObjectId)
        assert not doc.__changed__
        assert doc.__db__ is db

    loop.run_until_complete(test())


def test_save(loop, db):
    async def test():
        doc = Doc()
        doc.i = 13

        await db.save(doc)
        doc.i = 14
        await db.save(doc)

        assert await db.db.testdocs.find().count() == 1
        assert (await db.db.testdocs.find_one())['i'] == 14
        assert isinstance(doc.id, ObjectId)
        assert not doc.__changed__
        assert doc.__db__ is db

    loop.run_until_complete(test())


@pytest.fixture(scope='function')
def doc(loop, db):
    async def fixture():
        return await db.insert(
            Doc(
                b=True,
                i=13,
                l=[1, 2, 3],
            ),
        )

    return loop.run_until_complete(fixture())


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
def test_update_one(loop, db, doc, kwargs, result):
    async def test():
        await db.update_one(doc, **kwargs)
        raw_doc = await db.db['testdocs'].find_one(doc.id)

        assert raw_doc
        del raw_doc['_id']
        assert raw_doc == result

    loop.run_until_complete(test())


def test_remove(loop, db):
    async def test():
        col = db.db.testdocs
        await col.insert({'i': 13})

        doc = from_mongo(Doc, await col.find_one({'i': 13}))

        assert await col.count() == 1
        await db.remove(doc)
        assert await col.count() == 0

    loop.run_until_complete(test())
