import pytest
from bson import ObjectId

from yadm.documents import Document
from yadm import fields
from yadm.markers import NotLoaded


class TestDoc(Document):
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

    return db.get_queryset(TestDoc)


def test_count(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert qs.count() == 4


def test_len(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert len(qs) == 4


def test_find_one(qs):
    doc = qs.find_one({'i': 7})
    assert isinstance(doc, TestDoc)
    assert hasattr(doc, 'i')
    assert doc.i == 7


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
    qs.find({'i': {'$gte': 6}}).update({'$set': {'s': 'test'}})

    assert db.db.testdocs.count() == 10
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(10))
    assert db.db.testdocs.find({'s': 'test'}).count() == 4

    for doc in db.db.testdocs.find({'i': {'$lt': 6}}):
        assert doc['s'] != 'test'
        assert doc['s'].startswith('str(')

    for doc in db.db.testdocs.find({'i': {'$gte': 6}}):
        assert doc['s'] == 'test'


def test_update_not_multi(db, qs):
    qs.find({'i': {'$gte': 6}}).update({'$set': {'s': 'test'}}, multi=False)

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
    qs.find({'i': {'$gte': 6}}).remove()
    assert len([d for d in qs]) == 6
    assert {d.i for d in qs} == set(range(6))

    assert db.db.testdocs.count() == 6
    assert {d['i'] for d in db.db.testdocs.find()} == set(range(6))


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


def test_with_id(db, qs):
    _id = db.db.testdocs.find_one({'i': 4}, {'_id': True})['_id']
    doc = qs.with_id(_id)
    assert doc.s == 'str(4)'
    assert doc.i == 4


def test_contains(qs):
    doc = qs.find_one({'i': 0})
    assert doc in qs
    assert doc not in qs.find({'i': {'$ne': 0}})


def test_bulk(qs):
    bulk = qs.find({'i': {'$gte': 6}}).bulk()
    assert isinstance(bulk, dict)
    assert len(bulk), 4
    assert isinstance(list(bulk)[0], ObjectId)
    assert isinstance(list(bulk.values())[0], TestDoc)
    _id = list(bulk)[0]
    assert bulk[_id].id == _id
    assert {d.i for d in bulk.values()} == {6, 7, 8, 9}
