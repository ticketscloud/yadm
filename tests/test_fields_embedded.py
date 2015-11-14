import pytest
from bson import ObjectId

from yadm import fields
from yadm.documents import Document, EmbeddedDocument


class ETestDoc(EmbeddedDocument):
    id = fields.ObjectIdField(default_gen=True)
    i = fields.IntegerField()


class TestDoc(Document):
    __collection__ = 'testdoc'
    e = fields.EmbeddedDocumentField(ETestDoc, auto_create=False)


class TestDocAuto(Document):
    __collection__ = 'testdoc'
    e = fields.EmbeddedDocumentField(ETestDoc)


def test_default():
    doc = TestDoc()
    assert not hasattr(doc, 'e')


def test_default_auto():
    doc = TestDocAuto()
    assert hasattr(doc, 'e')
    assert isinstance(doc.e, ETestDoc)


def test_get(db):
    _id = db.db.testdoc.insert({'e': {'i': 13}})
    doc = db.get_queryset(TestDoc).with_id(_id)

    assert hasattr(doc, 'e')
    assert isinstance(doc.e, ETestDoc)
    assert hasattr(doc.e, 'i')
    assert isinstance(doc.e.i, int)
    assert doc.e.i == 13


def test_set():
    doc = TestDoc()
    doc.e = ETestDoc()

    assert hasattr(doc, 'e')
    assert isinstance(doc.e, ETestDoc)
    assert not hasattr(doc.e, 'i')

    doc.e.i = 13
    assert hasattr(doc.e, 'i')
    assert isinstance(doc.e.i, int)
    assert doc.e.i == 13


def test_set_typeerror():
    class FailETestDoc(EmbeddedDocument):
        s = fields.StringField

    doc = TestDoc()

    with pytest.raises(TypeError):
        doc.e = FailETestDoc()


def test_set_insert_wo_set(db):
    doc = TestDoc()
    db.insert(doc)

    data = db.db.testdoc.find_one({'_id': doc.id})
    assert set(data) == {'_id'}
    assert data['_id'] == doc.id


def test_set_insert(db):
    doc = TestDoc()
    doc.e = ETestDoc()
    doc.e.i = 13
    db.insert(doc)

    data = db.db.testdoc.find_one({'_id': doc.id})
    assert set(data) == {'_id', 'e'}
    assert data['_id'] == doc.id
    assert data['e']['i'] == 13


def test_set_save(db):
    _id = db.db.testdoc.insert({'e': {'i': 13}})
    doc = db.get_queryset(TestDoc).with_id(_id)

    doc.e.i = 26
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data == {'_id': _id, 'e': {'i': 26}}


def test_delete_save(db):
    _id = db.db.testdoc.insert({'e': {'i': 13}})
    doc = db.get_queryset(TestDoc).with_id(_id)

    del doc.e
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data == {'_id': _id}


def test_delete_deep_save(db):
    _id = db.db.testdoc.insert({'e': {'i': 13, 'id': ObjectId()}})
    doc = db.get_queryset(TestDoc).with_id(_id)

    del doc.e.i
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data == {'_id': _id, 'e': {'id': doc.e.id}}


def test_id_load_default():
    doc = TestDoc({'e': {'i': 13}})
    assert hasattr(doc, 'e')
    assert hasattr(doc.e, 'i')
    assert hasattr(doc.e, 'id')
    assert doc.e.i == 13
    assert isinstance(doc.e.id, ObjectId)
    assert doc.e.id is doc.e.id


def test_id_insert(db):
    doc = TestDoc({'e': {'i': 13}})

    # eid = doc.e.id
    db.insert(doc)
    # assert doc.e.id == eid
    # db.reload(doc)

    data = db.db.testdoc.find_one({'_id': doc.id})
    assert data['e']['id'] == doc.e.id
