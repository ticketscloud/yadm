import random

import pytest

from bson import ObjectId
import pymongo

from yadm.documents import Document
from yadm.log_items import Save, Insert
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
    col = db.db['testdocs']
    ids = [col.insert_one({'i': i}).inserted_id for i in range(10)]

    _id = ids[5]
    doc = db.get_document(Doc, _id)

    assert doc is not None
    assert doc._id == _id
    assert doc.i == 5
    assert doc.__db__ is db


def test_get_document__projection(db):
    col = db.db['testdocs']
    ids = [col.insert_one({'i': i, 'b': bool(i % 2)}).inserted_id
           for i in range(10)]

    _id = ids[5]
    doc = db.get_document(Doc, _id, projection={'b': False})

    assert doc is not None
    assert doc._id == _id
    assert doc.i == 5
    assert doc.__db__ is db

    with pytest.raises(fields.base.NotLoadedError):
        doc.b


def test_get_document__not_found(db):
    col = db.db['testdocs']
    [col.insert_one({'i': i}).inserted_id for i in range(10)]

    doc = db.get_document(Doc, ObjectId())

    assert doc is None


def test_get_document__not_found_exc(db):
    col = db.db['testdocs']
    [col.insert_one({'i': i}).inserted_id for i in range(10)]

    class Exc(Exception):
        pass

    with pytest.raises(Exc):
        db.get_document(Doc, ObjectId(), exc=Exc)


def test_insert_one(db):
    doc = Doc()
    doc.i = 13

    db.insert_one(doc)

    assert db.db['testdocs'].count_documents({}) == 1
    assert db.db['testdocs'].find()[0]['i'] == 13
    assert Insert(id=doc.id) in doc.__log__
    assert doc.__db__ is db


def test_insert_many(db):
    documents = [Doc(i=i) for i in range(10)]
    random.shuffle(documents)

    result = db.insert_many(documents)

    for _id, doc in zip(result.inserted_ids, documents):
        assert doc.id == _id

    assert len(result.inserted_ids) == len(documents)


def test_insert_many__unordered(db):
    documents = [Doc(i=i) for i in range(10)]
    random.shuffle(documents)

    result = db.insert_many(documents, ordered=False)

    for doc in documents:
        assert not hasattr(doc, 'id')

    assert len(result.inserted_ids) == len(documents)


def test_insert_many__empty(db):
    result = db.insert_many([])
    assert len(result.inserted_ids) == 0


def test_save_new(db):
    doc = Doc()
    doc.i = 13

    db.save(doc)

    assert db.db['testdocs'].count_documents({}) == 1
    assert db.db['testdocs'].find()[0]['i'] == 13
    assert Save(id=doc.id) in doc.__log__
    assert doc.__db__ is db


def test_save(db):
    col = db.db['testdocs']
    col.insert_one({'i': 13})

    doc = from_mongo(Doc, col.find_one({'i': 13}))
    doc.i = 26

    col.update_one({'_id': doc.id}, {'$set': {'b': True}})

    db.save(doc)

    assert doc.i == 26
    assert not hasattr(doc, 'b')
    assert db.db['testdocs'].count_documents({}) == 1
    assert db.db['testdocs'].find()[0]['i'] == 26
    assert Save(id=doc.id) in doc.__log__
    assert doc.__db__ is db


@pytest.fixture(scope='function')
def doc(db):
    doc = Doc(b=True, i=13, l=[1, 2, 3])
    db.insert_one(doc)
    return doc


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
def test_update_one(db, doc, kwargs, result):
    db.update_one(doc, **kwargs)
    raw_doc = db.db['testdocs'].find_one(doc.id)

    assert raw_doc
    del raw_doc['_id']
    assert raw_doc == result


def test_update_one__not_found(db, doc):
    assert db.update_one(doc) is None


def test_delete_one(db):
    col = db.db['testdocs']
    col.insert_one({'i': 13})

    doc = from_mongo(Doc, col.find_one({'i': 13}))

    assert col.count_documents({}) == 1
    db.delete_one(doc)
    assert col.count_documents({}) == 0


def test_reload(db):
    doc = Doc()
    doc.i = 1
    db.insert_one(doc)

    db.db['testdocs'].update_one({'_id': doc.id}, {'$set': {'i': 2}})

    assert doc.i == 1
    new = db.reload(doc)
    assert doc is new
    assert doc.i == 2


def test_reload_new_instance(db):
    doc = Doc()
    doc.i = 1
    db.insert_one(doc)

    db.db['testdocs'].update_one({'_id': doc.id}, {'$set': {'i': 2}})

    assert doc.i == 1
    new = db.reload(doc, new_instance=True)
    assert doc is not new
    assert doc.i == 1
    assert new.i == 2


def test_reload_w_projection(db):
    doc = Doc()
    doc.i = 1
    doc.b = True
    db.insert_one(doc)

    db.db['testdocs'].update_one({'_id': doc.id}, {'$set': {'i': 2}})

    assert doc.i == 1
    assert doc.b is True
    new = db.reload(doc, projection={'b': False})
    assert doc is new
    assert doc.i == 2

    with pytest.raises(fields.base.NotLoadedError):
        doc.b
