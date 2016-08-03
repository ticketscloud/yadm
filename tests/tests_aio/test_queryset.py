import pytest
from bson import ObjectId

from yadm import fields
from yadm.documents import Document


class Doc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField
    s = fields.StringField


@pytest.fixture
def qs(loop, db):
    async def fixture():
        for n in range(10):
            await db.db.testdocs.insert({
                'i': n,
                's': 'str({})'.format(n),
            })

    loop.run_until_complete(fixture())
    return db.get_queryset(Doc)


def test_aiter(loop, qs):
    async def test():
        n = 0
        async for doc in qs:
            n += 1
            assert isinstance(doc, Doc)
            assert isinstance(doc.id, ObjectId)

        assert n == 10

    loop.run_until_complete(test())


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
def test_slice(loop, qs, slice, result):
    async def test(qs=qs):
        if isinstance(result, type) and issubclass(result, Exception):
            with pytest.raises(result):
                qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))[slice]
        else:
            qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))[slice]
            docs = []
            async for doc in qs:
                docs.append(doc)

            assert [d.i for d in docs] == result

    loop.run_until_complete(test())


def test_get_one(loop, qs):
    async def test(qs=qs):
        qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
        doc = await qs[1]
        assert doc.i == 7

    loop.run_until_complete(test())


def test_get_one__index_error(loop, qs):
    async def test(qs=qs):
        qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
        with pytest.raises(IndexError):
            await qs[100]

    loop.run_until_complete(test())


def test_count(loop, qs):
    async def test(qs=qs):
        qs = qs.find({'i': {'$gte': 6}})
        assert await qs.count() == 4

    loop.run_until_complete(test())


def test_find_one(loop, qs):
    async def test():
        doc = await qs.find_one({'i': 7})
        assert isinstance(doc, Doc)
        assert hasattr(doc, 'i')
        assert doc.i == 7

    loop.run_until_complete(test())


def test_find_exc(loop, qs):
    class SimpleException(Exception):
        pass

    async def test():
        with pytest.raises(SimpleException):
            await qs.find_one({'i': 100500}, exc=SimpleException)

    loop.run_until_complete(test())


def test_distinct(loop, db, qs):
    async def test():
        res = await qs.distinct('i')
        assert isinstance(res, list)
        assert len(res) == len(set(res))

        await db.insert(Doc(i=3))
        await db.insert(Doc(i=4))
        await db.insert(Doc(i=10))

        res = await qs.distinct('i')
        assert len(res) == len(set(res))

    loop.run_until_complete(test())
