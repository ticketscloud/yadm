import pytest

from bson import ObjectId

from yadm.documents import Document
from yadm import fields


class DocRef(Document):
    __collection__ = 'testdocs_ref'
    i = fields.IntegerField()


class Doc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField()
    ref = fields.ReferenceField(DocRef)


@pytest.fixture
def id_ref_1(db):
    return db.db.testdocs_ref.insert_one({'i': 1}).inserted_id


@pytest.fixture
def id_ref_2(db, id_ref_1):
    return db.db.testdocs_ref.insert_one({'i': 2}).inserted_id


@pytest.fixture
def qs(db, id_ref_1, id_ref_2):
    for n in range(10):
        db.db.testdocs.insert_one({
            'i': n,
            'ref': id_ref_1 if n % 2 else id_ref_2,
        })

    return db(Doc)


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
    db.db.testdocs_ref.insert_one({'i': 3})
    qs = qs.join().get_queryset('ref')
    assert qs.count_documents() == 2
    assert {d.id for d in qs} == {id_ref_1, id_ref_2}


def test_get_queryset__bad_field_name(db, qs):
    with pytest.raises(ValueError):
        qs.join().get_queryset('bad_field')


def test_get_queryset__bad_field_type(db, qs):
    with pytest.raises(ValueError):
        qs.join().get_queryset('i')
