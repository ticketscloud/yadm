import random
import pytest
import pymongo
from bson import ObjectId

from yadm.documents import Document
from yadm import fields
from yadm.markers import NotLoaded
from yadm.results import UpdateResult, RemoveResult
from yadm.queryset import NotFoundError


class Doc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField
    s = fields.StringField


@pytest.fixture
def qs(db):
    for n in range(10):
        db.db.testdocs.insert({
            'i': n,
            's': 'str({})'.format(n),
        })

    return db.get_queryset(Doc)


def test_count(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert qs.count() == 4


def test_len(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert len(qs) == 4


@pytest.mark.parametrize('slice, result', [
    (slice(None, 2), [6, 7]),
    (slice(2, None), [8, 9]),
    (slice(1, 3), [7, 8]),
    (slice(100, 300), []),
    (slice(None, -3), TypeError),
    (slice(-3, None), TypeError),
    (slice(-3, -1), TypeError),
    (slice(None, None, 2), TypeError),
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
    assert qs.count() == 1
    assert qs[0].i == 5


def test_update(db, qs):
    result = qs.find({'i': {'$gte': 6}}).update({'$set': {'s': 'test'}})

    assert isinstance(result, UpdateResult)
    assert result
    assert result.matched == result.modified == int(result) == 4
    assert result.upserted == 0

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 4

    for doc in db.db.testdocs.find({'i': {'$lt': 6}}):
        assert doc['s'] != 'test'
        assert doc['s'].startswith('str(')

    for doc in db.db.testdocs.find({'i': {'$gte': 6}}):
        assert doc['s'] == 'test'


def test_update_not_multi(db, qs):
    result = qs.find({'i': {'$gte': 6}}).update({'$set': {'s': 'test'}}, multi=False)

    assert isinstance(result, UpdateResult)
    assert result
    assert result.matched == result.modified == int(result) == 1
    assert result.upserted == 0

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 1


def test_find_and_modify(db, qs):
    doc = qs({'i': 6}).find_and_modify({'$set': {'s': 'test'}})

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 1
    assert db.db.testdocs.find_one({'i': 6})['s'] == 'test'

    assert isinstance(doc, Document)
    assert doc.i == 6
    assert doc.s == 'str(6)'


def test_find_and_modify_new(db, qs):
    doc = qs({'i': 6}).find_and_modify(
        {'$set': {'s': 'test'}}, new=True)

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 1
    assert db.db.testdocs.find_one({'i': 6})['s'] == 'test'

    assert isinstance(doc, Document)
    assert doc.i == 6
    assert doc.s == 'test'


def test_find_and_modify_full_response(db, qs):
    result = qs({'i': 6}).find_and_modify(
        {'$set': {'s': 'test'}}, full_response=True)

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 1
    assert db.db.testdocs.find_one({'i': 6})['s'] == 'test'

    assert 'value' in result
    assert isinstance(result['value'], Document)
    assert result['value'].i == 6
    assert result['value'].s == 'str(6)'


def test_find_and_modify_full_response_new(db, qs):
    result = qs({'i': 6}).find_and_modify(
        {'$set': {'s': 'test'}}, full_response=True, new=True)

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 1
    assert db.db.testdocs.find_one({'i': 6})['s'] == 'test'

    assert 'value' in result
    assert isinstance(result['value'], Document)
    assert result['value'].i == 6
    assert result['value'].s == 'test'


def test_find_and_modify_sort(db, qs):
    qs = qs({'i': {'$lte': 6, '$gte': 4}}).sort(('s', -1))
    doc = qs.find_and_modify({'$set': {'s': 'test'}})

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 1
    assert db.db.testdocs.find_one({'i': 6})['s'] == 'test'

    assert isinstance(doc, Document)
    assert doc.i == 6
    assert doc.s == 'str(6)'


def test_find_and_modify_not_found(qs):
    ret = qs({'i': 13}).find_and_modify({'$set': {'s': 'test'}})
    assert ret is None


def test_remove(db, qs):
    result = qs.find({'i': {'$gte': 6}}).remove()

    assert isinstance(result, RemoveResult)
    assert result
    assert result.removed == int(result) == 4

    assert len([d for d in qs]) == 6
    assert {d.i for d in qs} == set(range(6))

    assert db.db.testdocs.count() == 6
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(6))


def test_remove__one(db, qs):
    result = qs.find({'i': {'$gte': 6}}).remove(multi=False)

    assert isinstance(result, RemoveResult)
    assert result
    assert result.removed == int(result) == 1

    assert len([d for d in qs]) == 9
    assert db.db.testdocs.count() == 9


def test_sort(qs):
    qs = qs.find({'i': {'$gte': 6}}).sort(('i', -1))
    assert [d.i for d in qs] == [9, 8, 7, 6]

    qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
    assert [d.i for d in qs] == [6, 7, 8, 9]


def test_fields(qs):
    doc = qs.fields('s').find_one({'i': 3})

    assert doc.s, 'str(3)'
    assert 'i' in doc.__raw__
    assert doc.__raw__['i'] is NotLoaded

    with pytest.raises(fields.NotLoadedError):
        doc.i


def test_fields_all(qs):
    doc = qs.fields('s').fields_all().find_one({'i': 3})
    assert 'i' in doc.__data__
    assert doc.__data__['i'] is 3
    assert doc.s == 'str(3)'
    assert doc.i == 3


def test_read_preference(db):
    sp = pymongo.read_preferences.ReadPreference.SECONDARY_PREFERRED
    qs = db(Doc).read_preference(sp)
    assert qs._collection.read_preference == sp


def test_with_id(db, qs):
    _id = db.db.testdocs.find_one({'i': 4}, {'_id': True})['_id']
    doc = qs.with_id(_id)
    assert doc.s == 'str(4)'
    assert doc.i == 4


def test_contains(qs):
    doc = qs.find_one({'i': 0})
    assert doc in qs
    assert doc not in qs.find({'i': {'$ne': 0}})


def test_ids(db):
    qs = db(Doc)
    assert not list(qs.ids())

    ids = set()
    ids.add(db.insert(Doc(i=3)))
    ids.add(db.insert(Doc(i=4)))
    ids.add(db.insert(Doc(i=10)))

    assert len(list(qs.ids())) == 3
    assert set(qs.ids()) == ids


def test_distinct(db, qs):
    res = qs.distinct('i')
    assert isinstance(res, list)
    assert len(res) == len(set(res))

    db.insert(Doc(i=3))
    db.insert(Doc(i=4))
    db.insert(Doc(i=10))

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
