import pytest

from pymongo.errors import BulkWriteError

from yadm import fields
from yadm.documents import Document
from yadm.bulk import Bulk


class TestDoc(Document):
    __collection__ = 'testdocs'

    i = fields.IntegerField()


@pytest.fixture
def index(db):
    return db.db.testdocs.ensure_index([('i', 1)], unique=True)


def test_create(db):
    bulk = db.bulk(TestDoc)
    assert isinstance(bulk, Bulk)


def test_insert_one(db):
    doc = TestDoc()
    doc.i = 1

    bulk = db.bulk(TestDoc)
    bulk.insert(doc)

    assert db.db.testdocs.count() == 0

    bulk.execute()

    assert bulk.result
    assert db.db.testdocs.count() == 1
    assert db.db.testdocs.find_one()['i'] == 1


def test_insert_many(db):
    bulk = db.bulk(TestDoc)

    for i in range(10):
        doc = TestDoc()
        doc.i = i
        bulk.insert(doc)

    assert db.db.testdocs.count() == 0
    bulk.execute()
    assert db.db.testdocs.count() == 10


def test_insert_type_error(db):
    class Doc(Document):
        pass

    bulk = db.bulk(TestDoc)

    with pytest.raises(TypeError):
        bulk.insert(Doc())


def test_context_manager(db):
    with db.bulk(TestDoc) as bulk:
        doc = TestDoc()
        doc.i = 1
        bulk.insert(doc)

    assert db.db.testdocs.count() == 1


def test_context_manager_error(db):
    with pytest.raises(RuntimeError):
        with db.bulk(TestDoc) as bulk:
            doc = TestDoc()
            doc.i = 1
            bulk.insert(doc)

            raise RuntimeError

    assert db.db.testdocs.count() == 0


def test_result(db):
    with db.bulk(TestDoc) as bulk:
        doc = TestDoc()
        doc.i = 1
        bulk.insert(doc)

    assert bulk.result.n_inserted == 1


def test_result_write_error(db, index):
    with db.bulk(TestDoc, raise_on_errors=False) as bulk:
        doc = TestDoc()
        doc.i = 1
        bulk.insert(doc)

        doc = TestDoc()
        doc.i = 2
        bulk.insert(doc)

        doc = TestDoc()
        doc.i = 1
        bulk.insert(doc)

    assert not bulk.result
    assert db.db.testdocs.count() == 2
    assert bulk.result.n_inserted == 2
    assert len(bulk.result.write_errors) == 1
    assert bulk.result.write_errors[0].document.i == 1


def test_result_write_error_raise(db, index):
    with pytest.raises(BulkWriteError):
        with db.bulk(TestDoc) as bulk:
            doc = TestDoc()
            doc.i = 1
            bulk.insert(doc)

            doc = TestDoc()
            doc.i = 2
            bulk.insert(doc)

            doc = TestDoc()
            doc.i = 1
            bulk.insert(doc)

    assert bulk.result.n_inserted == 2
    assert len(bulk.result.write_errors) == 1
    assert bulk.result.write_errors[0].document.i == 1


def test_result_write_error_ordered(db, index):
    with db.bulk(TestDoc, ordered=True, raise_on_errors=False) as bulk:
        for i in range(10):
            doc = TestDoc()
            doc.i = i
            bulk.insert(doc)

            doc = TestDoc()
            doc.i = 1
            bulk.insert(doc)

    assert db.db.testdocs.count() == 2
    assert bulk.result.n_inserted == 2
    assert len(bulk.result.write_errors) == 1
    assert bulk.result.write_errors[0].document.i == 1
