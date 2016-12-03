from bson import ObjectId

from yadm.queryset import BaseQuerySet
from yadm.results import UpdateResult, RemoveResult


class _AioQuerySetCursor:
    def __init__(self, qs):
        self.cursor = qs._cursor
        self.from_mongo_one = qs._from_mongo_one

    async def __anext__(self):
        raw = await self.cursor.__anext__()
        return self.from_mongo_one(raw)


class AioQuerySet(BaseQuerySet):
    def __len__(self):
        n = self.__class__.__name__
        raise TypeError("__len__ is not supported for {}".format(n))

    def __contains__(self, document):
        n = self.__class__.__name__
        raise TypeError("__contains__ is not supported for {}".format(n))

    async def __aiter__(self):
        return _AioQuerySetCursor(self)

    async def _get_one(self, index):
        cursor = self._cursor.skip(index).limit(1)
        try:
            raw = await cursor.__anext__()
        except StopAsyncIteration:
            raise IndexError(index)
        finally:
            await cursor.close()

        return self._from_mongo_one(raw)

    async def find_one(self, criteria=None, projection=None, *, exc=None):
        if isinstance(criteria, ObjectId):
            criteria = {'_id': criteria}

        qs = self.find(criteria=criteria, projection=projection)
        data = await self._collection.find_one(qs._criteria, qs._projection)

        if data is None:
            if exc is not None:
                raise exc(criteria)
            else:
                return None

        return self._from_mongo_one(data, projection=qs._projection)

    async def update(self, update, *, multi=True, upsert=False):
        raw_result = await self._collection.update(
            self._criteria,
            update,
            multi=multi,
            upsert=upsert,
        )
        return UpdateResult(raw_result)

    async def find_and_modify(
            self, update=None, *, upsert=False,
            full_response=False, new=False,
            **kwargs):
        result = await self._collection.find_and_modify(
            query=self._criteria,
            update=update,
            upsert=upsert,
            sort=self._sort or [],
            full_response=full_response,
            new=new,
            **kwargs
        )
        if not full_response:
            return self._from_mongo_one(result)
        else:
            result['value'] = self._from_mongo_one(result['value'])
            return result

    async def remove(self, *, multi=True):
        raw_result = await self._collection.remove(self._criteria, multi=multi)
        return RemoveResult(raw_result)

    async def distinct(self, field):
        return await self._cursor.distinct(field)

    async def count(self):
        return await self._cursor.count()
