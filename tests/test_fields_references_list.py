import pytest

from bson import ObjectId

from yadm.documents import Document
from yadm import fields
from yadm.fields.references_list import (
    ReferencesListField,
    ReferencesList,
    NotResolved,
    AlreadyResolved,
)


class RDoc(Document):
    __collection__ = 'testrdocs'
    i = fields.IntegerField()


class Doc(Document):
    __collection__ = 'testdocs'
    ref = ReferencesListField(RDoc)


@pytest.fixture(scope='function')
def docs(db):
    ref_one = RDoc()
    ref_one.i = 13
    db.insert_one(ref_one)

    ref_two = RDoc()
    ref_two.i = 42
    db.insert_one(ref_two)

    ref_three = RDoc()
    ref_three.i = 666
    db.insert_one(ref_three)

    doc_one = Doc()
    doc_one.ref.append(ref_one)
    doc_one.ref.append(ref_two)
    db.insert_one(doc_one)

    doc_two = Doc()
    doc_two.ref.append(ref_two)
    db.insert_one(doc_two)

    doc_three = Doc()
    doc_three.ref.append(ref_two)
    doc_three.ref.append(ref_three)
    db.insert_one(doc_three)

    return [doc_one, doc_two, doc_three]


def test_save_empty(db):
    doc = Doc()
    db.insert_one(doc)

    data = db.db.testdocs.find_one()
    assert data['ref'] == []


def test_save_new(db):
    doc = Doc()
    assert doc.ref is doc.ref

    ref_one = RDoc()
    ref_one.i = 13
    db.insert_one(ref_one)

    doc.ref.append(ref_one)

    ref_two = RDoc()
    ref_two.i = 42
    db.insert_one(ref_two)
    doc.ref.append(ref_two)

    db.insert_one(doc)

    data = db.db.testdocs.find_one()

    assert len(data['ref']) == 2
    assert isinstance(data['ref'][0], ObjectId)
    assert isinstance(data['ref'][1], ObjectId)
    assert data['ref'][0] == ref_one.id
    assert data['ref'][1] == ref_two.id


def test_resolve(db, docs):
    doc = db.get_document(Doc, docs[0].id)
    assert isinstance(doc.ref, ReferencesList)
    assert len(doc.ref) == 2
    assert len(doc.ref.ids) == 2
    assert len(doc.ref._documents) == 0
    assert not doc.ref.resolved

    doc.ref.resolve()
    assert doc.ref.resolved
    assert len(doc.ref) == 2
    assert len(doc.ref.ids) == 2
    assert len(doc.ref._documents) == 2

    for rdoc in doc.ref:
        assert isinstance(rdoc, RDoc)


def test_already_resolved(db, docs):
    doc = db.get_document(Doc, docs[0].id)
    doc.ref.resolve()

    with pytest.raises(AlreadyResolved):
        doc.ref.resolve()


def test_not_resolved(db, docs):
    doc = db.get_document(Doc, docs[0].id)
    rdoc = RDoc(_id=ObjectId(), i=10)

    with pytest.raises(NotResolved):
        doc.ref[0]

    with pytest.raises(NotResolved):
        doc.ref[0] = rdoc

    with pytest.raises(NotResolved):
        doc.ref.insert(0, rdoc)

    with pytest.raises(NotResolved):
        doc.ref.append(rdoc)

    with pytest.raises(NotResolved):
        doc.ref.pop(0)

    with pytest.raises(NotResolved):
        for doc in doc.ref:
            pass

    assert not doc.ref.resolved


def test_repr(db, docs):
    doc = db.get_document(Doc, docs[1].id)
    assert '[{!s}]'.format(docs[1].ref[0].id) in repr(doc.ref)
    doc.ref.resolve()
    assert '[{!r}]'.format(docs[1].ref[0]) in repr(doc.ref)


def test_setattr__reflist(db, docs):
    doc = Doc()
    doc.ref = docs[0].ref

    assert doc.ref == docs[0].ref


def test_setattr__list(db, docs):
    doc = Doc()
    doc.ref = list(docs[0].ref)

    assert doc.ref == docs[0].ref


def test_getitem():
    rdoc = RDoc(_id=ObjectId(), i=10)
    obj = ReferencesList(RDoc, [])
    obj.append(rdoc)

    assert obj[0] == rdoc


def test_setitem():
    rdoc_1 = RDoc(_id=ObjectId(), i=10)
    obj = ReferencesList(RDoc, [])
    obj.append(rdoc_1)

    rdoc_2 = RDoc(_id=ObjectId(), i=10)
    obj[0] = rdoc_2

    assert obj.ids == [rdoc_2.id]
    assert obj._documents == [rdoc_2]


def test_delitem():
    rdoc = RDoc(_id=ObjectId(), i=10)
    obj = ReferencesList(RDoc, [])
    obj.append(rdoc)
    del obj[0]

    assert len(obj) == 0
    assert not obj


def test_append():
    rdoc = RDoc(_id=ObjectId(), i=10)
    obj = ReferencesList(RDoc, [])
    obj.append(rdoc)

    assert obj.ids == [rdoc.id]
    assert obj._documents == [rdoc]


def test_insert():
    rdoc_1 = RDoc(_id=ObjectId(), i=10)
    rdoc_2 = RDoc(_id=ObjectId(), i=12)
    rdoc_3 = RDoc(_id=ObjectId(), i=13)

    obj = ReferencesList(RDoc, [])
    obj.insert(0, rdoc_1)
    obj.insert(0, rdoc_2)
    obj.insert(1, rdoc_3)

    assert obj.ids == [rdoc_2.id, rdoc_3.id, rdoc_1.id]
    assert obj._documents == [rdoc_2, rdoc_3, rdoc_1]


def test_pop():
    rdoc_1 = RDoc(_id=ObjectId(), i=10)
    rdoc_2 = RDoc(_id=ObjectId(), i=12)

    obj = ReferencesList(RDoc, [])
    obj.append(rdoc_1)
    obj.append(rdoc_2)

    res = obj.pop(1)
    assert res == rdoc_2
    assert obj.ids == [rdoc_1.id]
    assert obj._documents == [rdoc_1]
