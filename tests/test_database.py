import pytest

from bson import ObjectId
import pymongo

from yadm.documents import Document
from yadm.queryset import QuerySet
from yadm.serialize import from_mongo
from yadm import fields


class Doc(Document):
    __collection__ = 'testdocs'
    b = fields.BooleanField()
    i = fields.IntegerField()
    l = fields.ListField(fields.IntegerField())


def test_init(db):
    assert isinstance(db.client, pymongo.mongo_client.MongoClient)
    assert isinstance(db.db, pymongo.database.Database)
    assert db.db.name == 'test'
    assert db.name == 'test'


def test_get_collection(db):
    collection = db._get_collection(Doc)

    assert isinstance(collection, pymongo.collection.Collection)
    assert collection.name == 'testdocs'


def test_get_queryset(db):
    queryset = db.get_queryset(Doc)

    assert isinstance(queryset, QuerySet)
    assert queryset._db is db
    assert queryset._document_class is Doc


def test_get_document(db):
    col = db.db.testdocs
    ids = [col.insert_one({'i': i}).inserted_id for i in range(10)]

    _id = ids[5]
    doc = db.get_document(Doc, _id)

    assert doc is not None
    assert doc._id == _id
    assert doc.i == 5
    assert doc.__db__ is db


def test_get_document__not_found(db):
    col = db.db.testdocs
    [col.insert_one({'i': i}).inserted_id for i in range(10)]

    doc = db.get_document(Doc, ObjectId())

    assert doc is None


def test_get_document__not_found_exc(db):
    col = db.db.testdocs
    [col.insert_one({'i': i}).inserted_id for i in range(10)]

    class Exc(Exception):
        pass

    with pytest.raises(Exc):
        db.get_document(Doc, ObjectId(), exc=Exc)


def test_insert(db):
    doc = Doc()
    doc.i = 13

    db.insert(doc)

    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 13
    assert not doc.__changed__
    assert doc.__db__ is db


def test_save_new(db):
    doc = Doc()
    doc.i = 13

    db.save(doc)

    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 13
    assert not doc.__changed__
    assert doc.__db__ is db


def test_save(db):
    col = db.db.testdocs
    col.insert({'i': 13})

    doc = from_mongo(Doc, col.find_one({'i': 13}))
    doc.i = 26

    col.update({'_id': doc.id}, {'$set': {'b': True}})

    db.save(doc)

    assert doc.i == 26
    assert doc.b is True
    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 26
    assert db.db.testdocs.find()[0]['b'] is True
    assert not doc.__changed__
    assert doc.__db__ is db


def test_save_full(db):
    col = db.db.testdocs
    col.insert({'i': 13})

    doc = from_mongo(Doc, col.find_one({'i': 13}))
    doc.i = 26

    col.update({'_id': doc.id}, {'$set': {'b': True}})

    db.save(doc, full=True)

    assert doc.i == 26
    assert not hasattr(doc, 'b')
    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 26
    assert 'b' not in db.db.testdocs.find()[0]
    assert not doc.__changed__
    assert doc.__db__ is db


@pytest.mark.parametrize('unset', [['i'], {'i': True}])
def test_update_one(db, unset):
    col = db.db.testdocs
    _id = col.insert({'i': 13})

    doc = from_mongo(Doc, col.find_one(_id))
    db.update_one(doc, set={'b': True}, unset=unset)

    assert doc.b
    assert not hasattr(doc, 'i')


def test_update_one__inc(db):
    col = db.db.testdocs
    _id = col.insert({'i': 12})

    doc = from_mongo(Doc, col.find_one(_id))
    db.update_one(doc, inc={'i': 1})

    assert doc.i == 13
    assert not hasattr(doc, 'b')


@pytest.mark.parametrize('cmd, v, r', [
    ('push', 4, [1, 2, 3, 4]),
    ('pull', 2, [1, 3]),
])
def test_update_one__push_pull(db, cmd, v, r):
    col = db.db.testdocs
    _id = col.insert({'l': [1, 2, 3]})

    doc = from_mongo(Doc, col.find_one(_id))
    db.update_one(doc, **{cmd: {'l': v}})

    assert doc.l == r


def test_remove(db):
    col = db.db.testdocs
    col.insert({'i': 13})

    doc = from_mongo(Doc, col.find_one({'i': 13}))

    assert col.count() == 1
    db.remove(doc)
    assert col.count() == 0


def test_reload(db):
    doc = Doc()
    doc.i = 1
    db.insert(doc)

    db.db.testdocs.update({'_id': doc.id}, {'i': 2})

    assert doc.i == 1
    new = db.reload(doc)
    assert doc.i == 2
    assert doc is new


def test_reload_new_instance(db):
    doc = Doc()
    doc.i = 1
    db.insert(doc)

    db.db.testdocs.update({'_id': doc.id}, {'i': 2})

    assert doc.i == 1
    new = db.reload(doc, new_instance=True)
    assert doc.i == 1
    assert doc is not new
    assert new.i == 2
