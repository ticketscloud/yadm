import functools

from pymongo import (
    InsertOne,
    UpdateOne,
    UpdateMany,
    ReplaceOne,
    DeleteOne,
    DeleteMany,
)
from pymongo.results import BulkWriteResult

from .serialize import to_mongo

BATCH_SIZE = 1000

EMPTY_RESULT = BulkWriteResult(
    bulk_api_result={
        'nInserted': 0,
        'nMatched': 0,
        'nModified': 0,
        'nRemoved': 0,
        'nUpserted': 0,
        'upserted': []
    },
    acknowledged=True,
)


def _check_and_send(meth):
    @functools.wraps(meth)
    def wrapper(self, *args, **kwargs):
        meth(self, *args, **kwargs)
        if len(self._batch) >= self._batch_size:
            self.send_batch()

    return wrapper


class BulkWriter:
    def __init__(self, db, document_class,
                 ordered=False,
                 collection_params=None,
                 batch_size=BATCH_SIZE):
        self._db = db
        self._document_class = document_class
        self._ordered = ordered
        self._collection_params = collection_params
        self._batch_size = batch_size

        self._batch = []
        self._result = EMPTY_RESULT

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._batch:
            self.send_batch()

    def send_batch(self):
        data, self._batch = self._batch, []
        col = self._db._get_collection(self._document_class,
                                       self._collection_params)
        result = col.bulk_write(data, ordered=self._ordered)
        self._result = _union_results(self._result, result)

    @property
    def result(self):
        return self._result

    @_check_and_send
    def insert_one(self, document):
        self._batch.append(InsertOne(to_mongo(document)))

    @_check_and_send
    def update_one(self, cliteria, query, upsert=False):
        self._batch.append(UpdateOne(cliteria, query, upsert=upsert))

    @_check_and_send
    def update_many(self, cliteria, query, upsert=False):
        self._batch.append(UpdateMany(cliteria, query, upsert=upsert))

    @_check_and_send
    def replace_one(self, cliteria, document, upsert=False):
        self._batch.append(ReplaceOne(cliteria, to_mongo(document), upsert=upsert))

    @_check_and_send
    def delete_one(self, cliteria):
        self._batch.append(DeleteOne(cliteria))

    @_check_and_send
    def delete_many(self, cliteria):
        self._batch.append(DeleteMany(cliteria))

    def replace(self, document):
        self.replace_one({'_id': document.id}, document)

    def delete(self, document):
        self.delete_one({'_id': document.id})


def _union_results(first, second):
    acknowledged = first.acknowledged and second.acknowledged

    bulk_api_result = {
        'nInserted': _sum_ints(first, second, 'nInserted'),
        'nMatched': _sum_ints(first, second, 'nMatched'),
        'nModified': _sum_ints(first, second, 'nModified'),
        'nRemoved': _sum_ints(first, second, 'nRemoved'),
        'nUpserted': _sum_ints(first, second, 'nUpserted'),
        'upserted': (first.bulk_api_result['upserted'] +
                     second.bulk_api_result['upserted'])
    }
    return BulkWriteResult(bulk_api_result, acknowledged)


def _sum_ints(first, second, key):
    return ((first.bulk_api_result.get(key, 0) or 0) +
            (second.bulk_api_result.get(key, 0) or 0))
