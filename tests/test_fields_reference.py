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

    doc = db.get_queryset(TestDoc).find_one(id)

    assert doc._id == id
    assert isinstance(doc.ref, Document)
    assert doc.ref._id == id_ref


def test_get_broken(db):
    id = db.db.testdocs.insert({'ref': ObjectId()})
    doc = db.get_queryset(TestDoc).find_one(id)

    with pytest.raises(fields.BrokenReference):
        doc.ref


def test_get_notbindingtodatabase(db):
    id = db.db.testdocs.insert({'ref': ObjectId()})
    doc = db.get_queryset(TestDoc).find_one(id)
    doc.__db__ = None

    with pytest.raises(fields.NotBindingToDatabase):
        doc.ref


@pytest.mark.parametrize('cast', [
    lambda d: d,
    lambda d: d.id,
    lambda d: str(d.id),
])
def test_set(db, cast):
    id = db.db.testdocs.insert({})
    doc = db.get_queryset(TestDoc).find_one(id)
    doc.ref = cast(db.insert(TestDocRef()))

    assert isinstance(doc.ref, TestDocRef)

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

    doc = db.get_queryset(TestDocNotObjectId).find_one(id)

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

    doc = db.get_queryset(TestDocWEmbedded).find_one(id)

    assert doc._id == id
    assert isinstance(doc.emb, TestDocEmb)
    assert isinstance(doc.emb.ref, TestDocRef)
    assert doc.emb.ref._id == id_ref


def test_cache(db):
    ref_one = db.insert(TestDocRef(i=13))
    ref_two = db.insert(TestDocRef(i=26))

    db.insert(TestDoc(ref=ref_one))
    db.insert(TestDoc(ref=ref_one))
    db.insert(TestDoc(ref=ref_two))

    qs = db.get_queryset(TestDoc)
    assert len(qs.cache) == 0
    assert len(qs) == 3
    assert len({d.ref for d in qs}) == 2
    assert len({id(d.ref) for d in qs}) == 2
    assert len(qs.cache) == 2

    doc = qs.find_one()
    assert doc.ref.__qs__ is not doc.__qs__
    assert doc.ref.__qs__.cache is doc.__qs__.cache
