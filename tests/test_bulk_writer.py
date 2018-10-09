import sys

import pytest

from yadm.documents import Document
from yadm.fields import IntegerField
from yadm.bulk_writer import BATCH_SIZE


class Doc(Document):
    __collection__ = 'testdocs'
    i = IntegerField()


@pytest.fixture(scope='function')
def inserted(db):
    documents = [Doc(i=i) for i in range(10)]
    db.insert_many(documents)
    return documents


@pytest.mark.parametrize('batch_size', [3, BATCH_SIZE])
def test_insert_one(db, batch_size):
    with db.bulk_write(Doc, batch_size=batch_size) as writer:
        for doc in (Doc(i=i) for i in range(10)):
            writer.insert_one(doc)

    assert writer.result.inserted_count == 10
    assert db.db['testdocs'].count_documents({}) == 10


@pytest.mark.skipif(sys.version_info[:2] < (3, 6),
                    reason="Skip for Python < 3.6")
@pytest.mark.parametrize('batch_size', [3, BATCH_SIZE])
def test_complex(client, db, inserted, batch_size):
    with db.bulk_write(Doc, batch_size=batch_size) as writer:
        for doc in (Doc(i=i) for i in range(10, 20)):
            writer.insert_one(doc)

        writer.update_one({'i': 6}, {'$set': {'i': 66}})
        writer.update_many({'i': {'$gte': 18}}, {'$inc': {'i': 1}})
        writer.replace_one({'i': 3}, Doc(i=33))
        writer.delete_one({'i': 2})
        writer.delete_many({'i': {'$gte': 9, '$lt': 13}})

        doc = inserted[7]
        doc.i == 77
        writer.replace(doc)

        writer.delete(inserted[8])

    assert writer.result.inserted_count == 10
    assert writer.result.deleted_count == 6
    assert writer.result.modified_count == 6
    assert writer.result.upserted_count == 0

    assert db.db['testdocs'].count_documents({}) == 14
