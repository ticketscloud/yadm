import random

import pytest
import pymongo
from bson import ObjectId

from yadm import fields
from yadm.documents import Document
from yadm.queryset import NotFoundError


class Doc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField()
    s = fields.StringField()


@pytest.fixture
def qs(event_loop, db):
    async def fixture():
        for n in range(10):
            await db.db['testdocs'].insert_one({
                'i': n,
                's': 'str({})'.format(n),
            })

    event_loop.run_until_complete(fixture())
    return db.get_queryset(Doc)


@pytest.mark.asyncio
async def test_aiter(qs):
    n = 0
    async for doc in qs:
        n += 1
        assert isinstance(doc, Doc)
        assert isinstance(doc.id, ObjectId)

    assert n == 10


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
@pytest.mark.asyncio
async def test_slice(qs, slice, result):
    if isinstance(result, type) and issubclass(result, Exception):
        with pytest.raises(result):
            qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))[slice]
    else:
        qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))[slice]
        docs = []
        async for doc in qs:
            docs.append(doc)

        assert [d.i for d in docs] == result


@pytest.mark.asyncio
async def test_get_one(qs):
    qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
    doc = await qs[1]
    assert doc.i == 7


@pytest.mark.asyncio
async def test_get_one__index_error(qs):
    qs = qs.find({'i': {'$gte': 6}}).sort(('i', 1))
    with pytest.raises(IndexError):
        await qs[100]


@pytest.mark.asyncio
async def test_count_documents(qs):
    qs = qs.find({'i': {'$gte': 6}})
    assert await qs.count_documents() == 4


@pytest.mark.asyncio
async def test_find_one__query(qs):
    doc = await qs.find_one({'i': 7})
    assert isinstance(doc, Doc)
    assert hasattr(doc, 'i')
    assert doc.i == 7


@pytest.mark.asyncio
async def test_find_one__oid(qs):
    doc = await qs[3]
    res_doc = await qs.find_one(doc.id)
    assert isinstance(res_doc, Doc)
    assert res_doc.id == doc.id
    assert res_doc.i == doc.i


@pytest.mark.asyncio
async def test_find_one__nf(qs):
    res = await qs.find_one({'i': 100500})
    assert res is None


@pytest.mark.asyncio
async def test_find_one__nf_exc(qs):
    class SimpleException(Exception):
        pass
    with pytest.raises(SimpleException):
        await qs.find_one({'i': 100500}, exc=SimpleException)


@pytest.mark.asyncio
async def test_update_one(db, qs):
    result = await qs.find({
        'i': {'$in': [3, 7]},
    }).update_one({
        '$set': {'s': 'test'}
    })

    assert isinstance(result, pymongo.results.UpdateResult)
    assert result.acknowledged
    assert result.matched_count == result.modified_count == 1
    assert result.upserted_id is None

    assert (await db.db['testdocs'].count_documents({})) == 10
    assert {d['i'] async for d in db.db['testdocs'].find()} == set(range(10))
    assert (await db.db['testdocs'].count_documents({'s': 'test'})) == 1

    async for doc in db.db['testdocs'].find({'i': {'$nin': [3, 7]}}):
        assert doc['s'] != 'test'
        assert doc['s'].startswith('str(')

    doc_3 = await db.db['testdocs'].find_one({'i': 3})
    doc_7 = await db.db['testdocs'].find_one({'i': 7})
    assert doc_3['s'] == 'test' or doc_7['s'] == 'test'
    assert doc_3['s'] != 'test' or doc_7['s'] != 'test'


@pytest.mark.asyncio
async def test_update_many(db, qs):
    result = await qs.find({
        'i': {'$gte': 6}
    }).update_many({
        '$set': {'s': 'test'}
    })

    assert isinstance(result, pymongo.results.UpdateResult)
    assert result.acknowledged
    assert result.matched_count == result.modified_count == 4
    assert result.upserted_id is None

    assert (await db.db['testdocs'].count_documents({})) == 10
    assert {d['i'] async for d in db.db['testdocs'].find()} == set(range(10))
    assert (await db.db['testdocs'].count_documents({'s': 'test'})) == 4

    async for doc in db.db['testdocs'].find({'i': {'$lt': 6}}):
        assert doc['s'] != 'test'
        assert doc['s'].startswith('str(')

    async for doc in db.db['testdocs'].find({'i': {'$gte': 6}}):
        assert doc['s'] == 'test'


@pytest.mark.asyncio
async def test_delete_many(db, qs):
    result = await qs.find({'i': {'$gte': 6}}).delete_many()

    assert isinstance(result, pymongo.results.DeleteResult)
    assert result.acknowledged
    assert result.deleted_count == 4

    assert (await qs.count_documents()) == 6
    assert {d.i async for d in qs} == set(range(6))

    assert await db.db['testdocs'].count_documents({}) == 6
    assert {d['i'] async for d in db.db['testdocs'].find()} == set(range(6))


@pytest.mark.asyncio
async def test_delete_one(db, qs):
    result = await qs.find({'i': {'$gte': 6}}).delete_one()

    assert isinstance(result, pymongo.results.DeleteResult)
    assert result.acknowledged
    assert result.deleted_count == 1

    assert (await qs.count_documents()) == 9

    removed = list(set(range(10)) - {d.i async for d in qs})[0]

    assert await db.db['testdocs'].count_documents({}) == 9
    assert (await db.db['testdocs'].find_one({'i': removed})) is None


@pytest.mark.parametrize('return_document, i', [
    (pymongo.ReturnDocument.BEFORE, 9),
    (pymongo.ReturnDocument.AFTER, 99),
], ids=['BEFORE', 'AFTER'])
@pytest.mark.asyncio
async def test_find_one_and_update(db, qs, return_document, i):
    qs = qs.find({'i': {'$gte': 5}}).sort(('i', -1))
    doc = await qs.find_one_and_update({'$set': {'i': 99}},
                                       return_document=return_document)
    assert doc.i == i


@pytest.mark.parametrize('return_document, i', [
    (pymongo.ReturnDocument.BEFORE, 9),
    (pymongo.ReturnDocument.AFTER, 99),
], ids=['BEFORE', 'AFTER'])
@pytest.mark.asyncio
async def test_find_one_and_replace(db, qs, return_document, i):
    qs = qs.find({'i': {'$gte': 5}}).sort(('i', -1))
    doc = await qs.find_one_and_replace(Doc(i=99),
                                        return_document=return_document)
    assert doc.i == i


@pytest.mark.asyncio
async def test_find_one_and_delete(db, qs):
    qs = qs.find({'i': {'$gte': 5}}).sort(('i', -1))
    doc = await qs.find_one_and_delete()
    assert doc.i == 9


@pytest.mark.asyncio
async def test_hint(db, qs):
    await db.db[Doc.__collection__].create_index([('i', -1)])
    await db.db[Doc.__collection__].create_index([('s', 1)])

    _qs = qs.hint([('i', -1)])
    async for doc in _qs:
        assert doc

    _qs = qs.hint([('s', 1)])
    assert await _qs.count_documents()

    _qs = qs.hint([('wrong', 1)])
    with pytest.raises(pymongo.errors.OperationFailure):
        async for doc in _qs:
            assert False

    _qs = qs.hint([('wrong', 1)])
    with pytest.raises(pymongo.errors.OperationFailure):
        await _qs.count_documents()


@pytest.mark.asyncio
async def test_distinct(db, qs):
    res = await qs.distinct('i')
    assert isinstance(res, list)
    assert len(res) == len(set(res))

    await db.insert_one(Doc(i=3))
    await db.insert_one(Doc(i=4))
    await db.insert_one(Doc(i=10))

    res = await qs.distinct('i')
    assert len(res) == len(set(res))


@pytest.mark.asyncio
async def test_ids(qs):
    res = [i async for i in qs.ids()]
    assert len(res) == 10
    for i in res:
        assert isinstance(i, ObjectId)


@pytest.mark.asyncio
async def test_bulk(qs):
    bulk = await qs.find({'i': {'$gte': 6}}).bulk()
    assert isinstance(bulk, dict)
    assert len(bulk), 4
    assert isinstance(list(bulk)[0], ObjectId)
    assert isinstance(list(bulk.values())[0], Doc)
    _id = list(bulk)[0]
    assert bulk[_id].id == _id
    assert {d.i for d in bulk.values()} == {6, 7, 8, 9}


class TestFindIn:
    @pytest.fixture(autouse=True)
    def ids(self, event_loop, qs):
        async def fixture():
            ids = [doc.id async for doc in qs]
            random.shuffle(ids)
            return ids

        return event_loop.run_until_complete(fixture())

    @pytest.mark.asyncio
    async def test_simple(self, qs, ids):
        result = [getattr(doc, 'id', doc)
                  async for doc in qs.find_in(ids)]

        assert result == ids

    @pytest.mark.asyncio
    async def test_skip(self, qs, ids):
        ids.append('NotExistId')
        result = [doc.id async for doc in qs.find_in(ids)]
        assert result == ids[:-1]

    @pytest.mark.asyncio
    async def test_none(self, qs, ids):
        index = random.randrange(len(ids))
        ids[index] = 'NotExistId'
        result = [getattr(doc, 'id', doc)
                  async for doc in qs.find_in(ids, not_found='none')]

        ids[index] = None
        assert result == ids

    @pytest.mark.asyncio
    async def test_exception(self, qs, ids):
        [getattr(doc, 'id', doc)
            async for doc in qs.find_in(ids, not_found='error')]

        with pytest.raises(NotFoundError):
            ids.append('NotExistId')
            [getattr(doc, 'id', doc)
                async for doc in qs.find_in(ids, not_found='error')]

    @pytest.mark.asyncio
    async def test_copy_id(self, qs, ids):
        ids.append(ids[0])
        result = [getattr(doc, 'id', doc)
                  async for doc in qs.find_in(ids)]

        assert result == ids
