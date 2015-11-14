import pymongo

from yadm.documents import Document
from yadm.queryset import QuerySet
from yadm.serialize import from_mongo
from yadm import fields


class TestDoc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField()


def test_init(db):
    assert isinstance(db.client, pymongo.mongo_client.MongoClient)
    assert isinstance(db.db, pymongo.database.Database)
    assert db.db.name == 'test'
    assert db.name == 'test'


def test_get_collection(db):
    collection = db._get_collection(TestDoc)

    assert isinstance(collection, pymongo.collection.Collection)
    assert collection.name == 'testdocs'


def test_get_queryset(db):
    queryset = db.get_queryset(TestDoc)

    assert isinstance(queryset, QuerySet)
    assert queryset._db is db
    assert queryset._document_class is TestDoc


def test_insert(db):
    doc = TestDoc()
    doc.i = 13

    db.insert(doc)

    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 13
    assert not doc.__changed__
    assert doc.__db__ is db


def test_save_new(db):
    doc = TestDoc()
    doc.i = 13

    db.save(doc)

    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 13
    assert not doc.__changed__
    assert doc.__db__ is db


def test_save(db):
    col = db.db.testdocs
    col.insert({'i': 13})

    doc = from_mongo(TestDoc, col.find_one({'i': 13}))
    doc.i = 26

    db.save(doc)

    assert db.db.testdocs.find().count() == 1
    assert db.db.testdocs.find()[0]['i'] == 26
    assert not doc.__changed__
    assert doc.__db__ is db


def test_remove(db):
    col = db.db.testdocs
    col.insert({'i': 13})

    doc = from_mongo(TestDoc, col.find_one({'i': 13}))

    assert col.count() == 1
    db.remove(doc)
    assert col.count() == 0


def test_reload(db):
    doc = TestDoc()
    doc.i = 1
    db.insert(doc)

    db.db.testdocs.update({'_id': doc.id}, {'i': 2})

    assert doc.i == 1
    new = db.reload(doc)
    assert doc.i == 2
    assert doc is new


def test_reload_new_instance(db):
    doc = TestDoc()
    doc.i = 1
    db.insert(doc)

    db.db.testdocs.update({'_id': doc.id}, {'i': 2})

    assert doc.i == 1
    new = db.reload(doc, new_instance=True)
    assert doc.i == 1
    assert doc is not new
    assert new.i == 2
