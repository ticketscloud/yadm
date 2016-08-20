import pytest

from pymongo.errors import BulkWriteError

from yadm import fields
from yadm.documents import Document
from yadm.bulk import Bulk


class Doc(Document):
    __collection__ = 'docs'

    i = fields.IntegerField()


@pytest.fixture
def index(db):
    return db.db.docs.ensure_index([('i', 1)], unique=True)


def test_create(db):
    bulk = db.bulk(Doc)
    assert isinstance(bulk, Bulk)


def test_insert_one(db):
    doc = Doc()
    doc.i = 1

    bulk = db.bulk(Doc)
    bulk.insert(doc)

    assert db.db.docs.count() == 0

    bulk.execute()

    assert bulk.result
    assert db.db.docs.count() == 1
    assert db.db.docs.find_one()['i'] == 1


def test_insert_many(db):
    bulk = db.bulk(Doc)

    for i in range(10):
        doc = Doc()
        doc.i = i
        bulk.insert(doc)

    assert db.db.docs.count() == 0
    bulk.execute()
    assert db.db.docs.count() == 10


def test_insert_type_error(db):
    class OtherDoc(Document):
        __collection__ = 'otherdocs'

    bulk = db.bulk(Doc)

    with pytest.raises(TypeError):
        bulk.insert(OtherDoc())


def test_context_manager(db):
    with db.bulk(Doc) as bulk:
        doc = Doc()
        doc.i = 1
        bulk.insert(doc)

    assert db.db.docs.count() == 1


def test_context_manager_error(db):
    with pytest.raises(RuntimeError):
        with db.bulk(Doc) as bulk:
            doc = Doc()
            doc.i = 1
            bulk.insert(doc)

            raise RuntimeError

    assert db.db.docs.count() == 0


def test_update_one(db):
    doc_one = db.insert(Doc(i=1))
    doc_two = db.insert(Doc(i=2))

    with db.bulk(Doc) as bulk:
        bulk.update_one(doc_one, set={'i': 7})
        bulk.update_one(doc_two, inc={'i': 11})

    assert db.reload(doc_one).i == 7
    assert db.reload(doc_two).i == 13


def test_find_update(db):
    doc_one = db.insert(Doc(i=1))
    doc_two = db.insert(Doc(i=2))
    doc_three = db.insert(Doc(i=3))

    with db.bulk(Doc) as bulk:
        bulk.find({'i': {'$gte': 2}}).update(inc={'i': 11})

    assert db.reload(doc_one).i == 1
    assert db.reload(doc_two).i == 13
    assert db.reload(doc_three).i == 14


def test_find_upsert_update(db):
    with db.bulk(Doc) as bulk:
        bulk.find({'i': 10}).upsert().update(inc={'i': 3})

    assert len(db(Doc)) == 1
    assert db(Doc).find_one().i == 13


def test_find_remove(db):
    for i in range(20):
        db.insert(Doc(i=i))

    with db.bulk(Doc) as bulk:
        bulk.find({'i': 13}).remove()
        bulk.find({'i': {'$lt': 7}}).remove()
        bulk.find({'i': {'$gte': 15}}).remove()

    assert len(db(Doc)) == 7
    assert set(db(Doc).distinct('i')) == {7, 8, 9, 10, 11, 12, 14}


def test_find_mixed(db):
    for i in range(5):
        db.insert(Doc(i=i))

    with db.bulk(Doc, ordered=True) as bulk:
        for i in range(5, 10):
            bulk.insert(Doc(i=i))

        assert len(db(Doc)) == 5
        bulk.find({'i': {'$gte': 7}}).update(inc={'i': 10})
        assert len(db(Doc)) == 5

        bulk.find({'i': {'$lt': 4}}).remove()
        assert len(db(Doc)) == 5

    assert len(db(Doc)) == 6
    assert set(db(Doc).distinct('i')) == {4, 5, 6, 17, 18, 19}


def test_result(db):
    with db.bulk(Doc) as bulk:
        doc = Doc()
        doc.i = 1
        bulk.insert(doc)

    assert bulk.result.n_inserted == 1


def test_result_write_error(db, index):
    with db.bulk(Doc, raise_on_errors=False) as bulk:
        doc = Doc()
        doc.i = 1
        bulk.insert(doc)

        doc = Doc()
        doc.i = 2
        bulk.insert(doc)

        doc = Doc()
        doc.i = 1
        bulk.insert(doc)

    assert not bulk.result
    assert db.db.docs.count() == 2
    assert bulk.result.n_inserted == 2
    assert len(bulk.result.write_errors) == 1
    assert bulk.result.write_errors[0].document.i == 1


def test_result_write_error_raise(db, index):
    with pytest.raises(BulkWriteError):
        with db.bulk(Doc) as bulk:
            doc = Doc()
            doc.i = 1
            bulk.insert(doc)

            doc = Doc()
            doc.i = 2
            bulk.insert(doc)

            doc = Doc()
            doc.i = 1
            bulk.insert(doc)

    assert bulk.result.n_inserted == 2
    assert len(bulk.result.write_errors) == 1
    assert bulk.result.write_errors[0].document.i == 1


def test_result_write_error_ordered(db, index):
    with db.bulk(Doc, ordered=True, raise_on_errors=False) as bulk:
        for i in range(10):
            doc = Doc()
            doc.i = i
            bulk.insert(doc)

            doc = Doc()
            doc.i = 1
            bulk.insert(doc)

    assert db.db.docs.count() == 2
    assert bulk.result.n_inserted == 2
    assert len(bulk.result.write_errors) == 1
    assert bulk.result.write_errors[0].document.i == 1


def test_repr(db):
    assert 'Bulk' in repr(db.bulk(Doc))


def test_find_repr(db):
    assert 'BulkQuery' in repr(db.bulk(Doc).find({'i': 1}))
    assert ': 13' in repr(db.bulk(Doc).find({'i': 13}))
    assert 'upsertable' in repr(db.bulk(Doc).find({'i': 13}).upsert())
