import pytest

from bson import ObjectId

from yadm.documents import Document, EmbeddedDocument
from yadm import fields


class TestDocRef(Document):
    __collection__ = 'testdocs_ref'


class TestDoc(Document):
    __collection__ = 'testdocs'
    ref = fields.ReferenceField(TestDocRef)


def test_get(db):
    id_ref = db.db.testdocs_ref.insert({})
    id = db.db.testdocs.insert({'ref': id_ref})

    doc = db.get_queryset(TestDoc).with_id(id)

    assert doc._id == id
    assert isinstance(doc.ref, Document)
    assert doc.ref._id == id_ref


def test_get_broken(db):
    id = db.db.testdocs.insert({'ref': ObjectId()})
    doc = db.get_queryset(TestDoc).with_id(id)

    with pytest.raises(fields.BrokenReference):
        doc.ref


def test_get_notbindingtodatabase(db):
    id = db.db.testdocs.insert({'ref': ObjectId()})
    doc = db.get_queryset(TestDoc).with_id(id)
    doc.__db__ = None

    with pytest.raises(fields.NotBindingToDatabase):
        doc.ref


def test_set_objectid(db):
    id = db.db.testdocs.insert({})
    doc = db.get_queryset(TestDoc).with_id(id)
    doc.ref = db.db.testdocs_ref.insert({})

    assert isinstance(doc.ref, TestDocRef)

    db.save(doc)

    id_ref = db.db.testdocs.find_one({'_id': id})['ref']

    assert isinstance(id_ref, ObjectId)
    assert isinstance(doc.ref, Document)
    assert id_ref == doc.ref.id


def test_set_doc(db):
    id = db.db.testdocs.insert({})
    doc = db.get_queryset(TestDoc).with_id(id)

    doc_ref = TestDocRef()
    db.save(doc_ref)

    doc.ref = doc_ref
    db.save(doc)

    id_ref = db.db.testdocs.find_one({'_id': id})['ref']

    assert isinstance(id_ref, ObjectId)
    assert isinstance(doc.ref, Document)
    assert id_ref == doc.ref.id


def test_not_object_id_get(db):
    class TestDocNotObjectIdRef(Document):
        __collection__ = 'testdocs_ref'
        _id = fields.IntegerField()

    class TestDocNotObjectId(Document):
        __collection__ = 'testdocs'
        ref = fields.ReferenceField(TestDocNotObjectIdRef)

    id_ref = db.db.testdocs_ref.insert({'_id': 13})
    id = db.db.testdocs.insert({'ref': id_ref})

    doc = db.get_queryset(TestDocNotObjectId).with_id(id)

    assert doc._id == id
    assert isinstance(doc.ref, Document)
    assert doc.ref._id == id_ref


class TestDocEmb(EmbeddedDocument):
    ref = fields.ReferenceField(TestDocRef)


class TestDocWEmbedded(Document):
    __collection__ = 'testdocs'
    emb = fields.EmbeddedDocumentField(TestDocEmb)


def test_embedded_get(db):
    id_ref = db.db.testdocs_ref.insert({})
    id = db.db.testdocs.insert({'emb': {'ref': id_ref}})

    doc = db.get_queryset(TestDocWEmbedded).with_id(id)

    assert doc._id == id
    assert isinstance(doc.emb, TestDocEmb)
    assert isinstance(doc.emb.ref, TestDocRef)
    assert doc.emb.ref._id == id_ref
