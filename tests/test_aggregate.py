from random import randint
from unittest.mock import Mock, MagicMock

import pytest

from yadm import Document
from yadm import fields
from yadm.aggregation import Aggregator, AgOperator


class Doc(Document):
    __collection__ = 'docs'
    i = fields.IntegerField()


@pytest.fixture(scope='function')
def docs(db):
    with db.bulk_write(Doc) as writer:
        docs = []
        for n in range(randint(10, 20)):
            doc = Doc(i=randint(-666, 666))
            writer.insert_one(doc)
            docs.append(doc)

    return docs


@pytest.fixture(scope='module')
def fake_aggregator():
    Doc = MagicMock(name='Doc')  # noqa
    Doc.__collection__ = 'col'
    return Aggregator(Mock(), Doc)


def test_pipeline(db, docs):
    agg = db.aggregate(Doc).match(i={'$gt': 0}).group({'_id': '1', 's': {'$sum': '$i'}})
    assert agg._pipeline == [
        {'$match': {'i': {'$gt': 0}}},
        {'$group': {'_id': '1', 's': {'$sum': '$i'}}},
    ]


def test_first(db, docs):
    agg = db.aggregate(Doc).group({
        '_id': '1',
        's': {'$sum': '$i'},
        'c': {'$sum': 1},
    })
    res = agg[0]

    assert set(res) == {'_id', 'c', 's'}
    assert res['c'] == len(docs)
    assert res['s'] == sum(d.i for d in docs)


def test_index(db, docs):
    agg = db.aggregate(Doc).sort(i=1)
    result = []
    for idx, _ in enumerate(agg):
        result.append(agg[idx]['i'])

    assert result == sorted([d.i for d in docs])


def test_indexerror(db, docs):
    agg = db.aggregate(Doc)

    with pytest.raises(IndexError):
        agg[100500]


def test_negativeindex(fake_aggregator):
    with pytest.raises(NotImplementedError):
        fake_aggregator[-1]


def test_slice(db, docs):
    numbers = sorted([d.i for d in docs])
    agg = db.aggregate(Doc).sort(i=1)
    assert [d['i'] for d in agg[1:3]] == numbers[1:3]


def test_slicestep(fake_aggregator):
    with pytest.raises(NotImplementedError):
        fake_aggregator[1:3:2]


def test_indextypeerror(fake_aggregator):
    with pytest.raises(TypeError):
        fake_aggregator['1']


def test_for(db, docs):
    agg = db.aggregate(Doc).match(i={'$gt': 0}).project(n='$i')
    count = 0

    for item in agg:
        assert item['n'] > 0
        count += 1

    assert count == len([d.i for d in docs if d.i > 0])


def test_repr(fake_aggregator):
    assert 'Aggregator' in repr(fake_aggregator)
    assert 'col' in repr(fake_aggregator)

    agop = AgOperator(fake_aggregator, 'operator')
    assert 'AgOperator' in repr(agop)
    assert 'operator' in repr(agop)
    assert 'col' in repr(agop)


def test_empty(fake_aggregator):
    with pytest.raises(ValueError):
        fake_aggregator.match()


def test_error(fake_aggregator):
    with pytest.raises(ValueError):
        fake_aggregator.match({'a': 1}, b=2)
