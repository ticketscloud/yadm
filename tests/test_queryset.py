import random

import pytest

import pymongo
from bson import ObjectId

from yadm import fields
from yadm.documents import Document
from yadm.queryset import QuerySet, NotFoundError
from yadm.exceptions import NotLoadedError


class Doc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField()
    s = fields.StringField()


@pytest.fixture
def qs(db):
    for n in range(10):
        db.db['testdocs'].insert_one({
            'i': n,
            's': 'str({})'.format(n),
        })

    return db.get_queryset(Doc)


def test_repr(qs):
    assert Doc.__collection__ in repr(qs)


def test_count(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert qs.count_documents() == 4


def test_len(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert len(qs) == 4


@pytest.mark.parametrize('additional_qs, result', [
    ({}, True),
    ({'i': {'$gte': 6}}, True),
    ({'i': {'$gte': 1000}}, False),
])
def test_bool_(qs, additional_qs, result):
    assert bool(qs.find(additional_qs)) is result


@pytest.mark.parametrize('slice, result', [
    (slice(None, 2), [6, 7]),
    (slice(2, None), [8, 9]),
    (slice(1, 3), [7, 8]),
    (slice(100, 300), []),
    (slice(None, -3), TypeError),
    (slice(-3, None), TypeError),
    (slice(-3, -1), TypeError),
    (slice(None, None, 2), TypeError),
    ('some_wrong', TypeError),
])
def test_slice(qs, slice, result):
    if isinstance(result, type) and issubclass(result, Exception):
        with pytest.raises(result):
            qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))[slice]
    else:
        qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))[slice]
        assert [d.i for d in qs] == result


def test_get_one(qs):
    qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
    doc = qs[1]
    assert doc.i == 7


def test_get_one__index_error(qs):
    qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
    with pytest.raises(IndexError):
        qs[100]


def test_find_one(qs):
    doc = qs.find_one({'i': 7})
    assert isinstance(doc, Doc)
    assert hasattr(doc, 'i')
    assert doc.i == 7


def test_find_exc(qs):
    class SimpleException(Exception):
        pass

    with pytest.raises(SimpleException):
        qs.find_one({'i': 100500}, exc=SimpleException)


def test_find(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert len([d for d in qs]) == 4
    assert {d.i for d in qs} == {6, 7, 8, 9}


def test_find_with_collisium(qs):
    qs = qs.find({'i': {'$gt': 4}})
    qs = qs.find({'i': {'$lt': 6}})
    assert qs.count_documents() == 1
    assert qs[0].i == 5


def test_update_many(db, qs):
    result = qs.find({'i': {'$gte': 6}}).update_many({'$set': {'s': 'test'}})

    assert isinstance(result, pymongo.results.UpdateResult)
    assert result.acknowledged
    assert result.matched_count == result.modified_count == 4
    assert result.upserted_id is None

    assert db.db['testdocs'].count_documents({}) == 10
    assert {d['i'] for d in db.db['testdocs'].find()} == set(range(10))
    assert db.db['testdocs'].count_documents({'s': 'test'}) == 4

    for doc in db.db['testdocs'].find({'i': {'$lt': 6}}):
        assert doc['s'] != 'test'
        assert doc['s'].startswith('str(')

    for doc in db.db['testdocs'].find({'i': {'$gte': 6}}):
        assert doc['s'] == 'test'


def test_update_one(db, qs):
    result = qs.find({'i': {'$gte': 6}}).update_one({'$set': {'s': 'test'}})

    assert isinstance(result, pymongo.results.UpdateResult)
    assert result.acknowledged
    assert result.matched_count == result.modified_count == 1
    assert result.upserted_id is None

    assert db.db['testdocs'].count_documents({}) == 10
    assert {d['i'] for d in db.db['testdocs'].find()} == set(range(10))
    assert db.db['testdocs'].count_documents({'s': 'test'}) == 1


def test_delete_many(db, qs):
    result = qs.find({'i': {'$gte': 6}}).delete_many()

    assert isinstance(result, pymongo.results.DeleteResult)
    assert result.acknowledged
    assert result.deleted_count == 4

    assert len([d for d in qs]) == 6
    assert {d.i for d in qs} == set(range(6))

    assert db.db['testdocs'].count_documents({}) == 6
    assert {d['i'] for d in db.db['testdocs'].find()} == set(range(6))


def test_delete_one(db, qs):
    result = qs.find({'i': {'$gte': 6}}).delete_one()

    assert isinstance(result, pymongo.results.DeleteResult)
    assert result.acknowledged
    assert result.deleted_count == 1

    assert len([d for d in qs]) == 9
    assert db.db['testdocs'].count_documents({}) == 9


@pytest.mark.parametrize('return_document, i', [
    (pymongo.ReturnDocument.BEFORE, 9),
    (pymongo.ReturnDocument.AFTER, 99),
], ids=['BEFORE', 'AFTER'])
def test_find_one_and_update(db, qs, return_document, i):
    qs = qs.find({'i': {'$gte': 5}}).sort(('i', -1))
    doc = qs.find_one_and_update({'$set': {'i': 99}},
                                 return_document=return_document)
    assert doc.i == i


@pytest.mark.parametrize('return_document, i', [
    (pymongo.ReturnDocument.BEFORE, 9),
    (pymongo.ReturnDocument.AFTER, 99),
], ids=['BEFORE', 'AFTER'])
def test_find_one_and_replace(db, qs, return_document, i):
    qs = qs.find({'i': {'$gte': 5}}).sort(('i', -1))
    doc = qs.find_one_and_replace(Doc(i=99),
                                  return_document=return_document)
    assert doc.i == i


def test_find_one_and_delete(db, qs):
    qs = qs.find({'i': {'$gte': 5}}).sort(('i', -1))
    doc = qs.find_one_and_delete()
    assert doc.i == 9


def test_hint(db, qs):
    db.db[Doc.__collection__].create_index([('i', -1)])
    db.db[Doc.__collection__].create_index([('s', 1)])

    _qs = qs.hint([('i', -1)])
    assert list(_qs)

    _qs = qs.hint([('s', 1)])
    assert len(_qs)

    _qs = qs.hint([('wrong', 1)])
    with pytest.raises(pymongo.errors.OperationFailure):
        list(_qs)

    _qs = qs.hint([('wrong', 1)])
    with pytest.raises(pymongo.errors.OperationFailure):
        len(_qs)


def test_sort(qs):
    qs = qs.find({'i': {'$gte': 6}}).sort(('i', -1))
    assert [d.i for d in qs] == [9, 8, 7, 6]

    qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
    assert [d.i for d in qs] == [6, 7, 8, 9]


def test_fields(qs):
    doc = qs.fields('s').find_one({'i': 3})

    assert doc.s, 'str(3)'
    assert 'i' not in doc.__raw__
    assert 'i' in doc.__not_loaded__

    with pytest.raises(NotLoadedError):
        doc.i


def test_fields_all(qs):
    doc = qs.fields('s').fields_all().find_one({'i': 3})
    assert 'i' in doc.__raw__
    assert doc.__raw__['i'] == 3
    assert doc.s == 'str(3)'
    assert doc.i == 3


@pytest.mark.parametrize('projection', [
    {'i': True},
    {'s': False},
], ids=['positive', 'negative'])
def test_projection(qs, projection):
    doc = qs.find_one({'i': 3}, projection)
    assert 'i' in doc.__raw__
    assert doc.__raw__['i'] == 3
    assert 's' not in doc.__raw__

    with pytest.raises(NotLoadedError):
        doc.s


def test_read_preference(db):
    sp = pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED
    qs = db(Doc).read_preference(sp)
    assert qs._collection.read_preference == sp


def test_contains(qs):
    doc = qs.find_one({'i': 0})
    assert doc in qs
    assert doc not in qs.find({'i': {'$ne': 0}})


def test_ids(db):
    qs = db(Doc)
    assert not list(qs.ids())

    ids = set()

    _doc = Doc(i=3)
    db.insert_one(_doc)
    ids.add(_doc)

    _doc = Doc(i=4)
    db.insert_one(_doc)
    ids.add(_doc)

    _doc = Doc(i=10)
    db.insert_one(_doc)
    ids.add(_doc)

    assert len(list(qs.ids())) == 3
    assert set(qs.ids()) == ids


def test_distinct(db, qs):
    res = qs.distinct('i')
    assert isinstance(res, list)
    assert len(res) == len(set(res))

    db.insert_one(Doc(i=3))
    db.insert_one(Doc(i=4))
    db.insert_one(Doc(i=10))

    res = qs.distinct('i')
    assert len(res) == len(set(res))


def test_bulk(qs):
    bulk = qs.find({'i': {'$gte': 6}}).bulk()
    assert isinstance(bulk, dict)
    assert len(bulk), 4
    assert isinstance(list(bulk)[0], ObjectId)
    assert isinstance(list(bulk.values())[0], Doc)
    _id = list(bulk)[0]
    assert bulk[_id].id == _id
    assert {d.i for d in bulk.values()} == {6, 7, 8, 9}


@pytest.mark.parametrize('preferred, rp', [
    (False, pymongo.read_preferences.Primary()),
    (True, pymongo.read_preferences.PrimaryPreferred()),
])
def test_read_primary(qs, preferred, rp):
    assert not qs._collection_params
    qs = qs.read_primary(preferred=preferred)
    assert qs._collection_params['read_preference'] == rp
    assert len(qs)  # try to execute query


def test_batch_size__set():
    qs_one = QuerySet(None, Doc)
    assert qs_one._batch_size is None

    qs_two = qs_one.batch_size(10)
    assert qs_two._batch_size == 10
    assert qs_one._batch_size is None

    qs_three = qs_two.batch_size(None)
    assert qs_three._batch_size is None
    assert qs_two._batch_size == 10
    assert qs_one._batch_size is None


def test_batch_size__get(qs):
    n = 0
    for doc in qs.batch_size(10):
        n += 1

    assert n > 0


def test_default_projection(db, qs):
    class ProjectedDoc(Doc):
        __default_projection__ = {'s': False}

    qs = db.get_queryset(ProjectedDoc)
    doc = qs.find_one()

    assert isinstance(doc.i, int)

    with pytest.raises(NotLoadedError):
        doc.s


class TestFindIn:
    @pytest.fixture(autouse=True)
    def ids(self, qs):
        ids = [doc.id for doc in qs]
        random.shuffle(ids)
        return ids

    def test_simple(self, qs, ids):
        result = [getattr(doc, 'id', doc)
                  for doc in qs.find_in(ids)]

        assert result == ids

    def test_skip(self, qs, ids):
        ids.append('NotExistId')
        result = [doc.id for doc in qs.find_in(ids)]
        assert result == ids[:-1]

    def test_none(self, qs, ids):
        index = random.randrange(len(ids))
        ids[index] = 'NotExistId'
        result = [getattr(doc, 'id', doc)
                  for doc in qs.find_in(ids, not_found='none')]

        ids[index] = None
        assert result == ids

    def test_exception(self, qs, ids):
        [getattr(doc, 'id', doc)
         for doc in qs.find_in(ids, not_found='error')]

        with pytest.raises(NotFoundError):
            ids.append('NotExistId')
            [getattr(doc, 'id', doc)
             for doc in qs.find_in(ids, not_found='error')]

    def test_copy_id(self, qs, ids):
        ids.append(ids[0])
        result = [getattr(doc, 'id', doc)
                  for doc in qs.find_in(ids)]

        assert result == ids
