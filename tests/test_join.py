import pytest

from bson import ObjectId

from yadm.documents import Document
from yadm import fields


class TestDocRef(Document):
    __collection__ = 'testdocs_ref'
    i = fields.IntegerField


class TestDoc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField
    ref = fields.ReferenceField(TestDocRef)


@pytest.fixture
def id_ref_1(db):
    return db.db.testdocs_ref.insert({'i': 1})


@pytest.fixture
def id_ref_2(db, id_ref_1):
    return db.db.testdocs_ref.insert({'i': 2})


@pytest.fixture
def qs(db, id_ref_1, id_ref_2):
    for n in range(10):
        db.db.testdocs.insert({
            'i': n,
            'ref': id_ref_1 if n % 2 else id_ref_2,
        })

    return db(TestDoc)


@pytest.fixture
def join(qs):
    return qs.join('ref')


def test_len(join):
    assert len(join) == 10


def test_iter(join):
    assert len(list(join)) == 10


def test_join(join, id_ref_1, id_ref_2):
    for doc in join:
        assert 'ref' in doc.__raw__
        assert isinstance(doc.__raw__['ref'], ObjectId)
        assert 'ref' in doc.__cache__
        ref = doc.__cache__['ref']
        assert isinstance(ref, Document)
        assert doc.ref.id == id_ref_1 if doc.i % 2 else id_ref_2


def test_get_queryset(db, qs, id_ref_1, id_ref_2):
    db.db.testdocs_ref.insert({'i': 3})
    qs = qs.join().get_queryset('ref')
    assert qs.count() == 2
    assert {d.id for d in qs} == {id_ref_1, id_ref_2}
