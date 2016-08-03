import pytest
from bson import ObjectId

from yadm import fields
from yadm.documents import Document
from yadm.serialize import from_mongo


class Doc(Document):
    __collection__ = 'testdocs'
    b = fields.BooleanField()
    i = fields.IntegerField()


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

    loop.run_until_complete((test()))


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

    loop.run_until_complete((test()))


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

    loop.run_until_complete((test()))


@pytest.mark.parametrize('unset', [['i'], {'i': True}])
def test_update_one(loop, db, unset):
    async def test():
        col = db.db.testdocs
        _id = await col.insert({'i': 13})

        doc = from_mongo(Doc, await col.find_one(_id))
        await db.update_one(doc, set={'b': True}, unset=unset)

        assert doc.b
        assert not hasattr(doc, 'i')

    loop.run_until_complete((test()))


def test_remove(loop, db):
    async def test():
        col = db.db.testdocs
        await col.insert({'i': 13})

        doc = from_mongo(Doc, await col.find_one({'i': 13}))

        assert await col.count() == 1
        await db.remove(doc)
        assert await col.count() == 0

    loop.run_until_complete((test()))
