from bson import ObjectId

from yadm.queryset import BaseQuerySet, NotFoundBehavior, NotFoundError
from yadm.results import UpdateResult, RemoveResult


class AioQuerySet(BaseQuerySet):
    async def __aiter__(self):
        async for raw in self._cursor:
            yield self._from_mongo_one(raw)

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
        else:  # pragma: no cover
            result['value'] = self._from_mongo_one(result['value'])
            return result

    async def remove(self, *, multi=True):
        raw_result = await self._collection.remove(self._criteria, multi=multi)
        return RemoveResult(raw_result)

    async def distinct(self, field):
        return await self._cursor.distinct(field)

    async def count(self):
        return await self._cursor.count()

    async def ids(self):
        async for raw in self.copy(projection={'_id': True})._cursor:
            yield raw['_id']

    async def bulk(self):
        qs = self.copy()
        qs._sort = None
        return {obj.id: obj async for obj in qs}

    async def join(self, *field_names):  # pragma: no cover
        raise NotImplementedError

    async def find_in(self, comparable, field='_id', *,
                      not_found=NotFoundBehavior.SKIP):
        not_found = NotFoundBehavior(not_found)
        hash_docs = {}

        async for doc in self.find({field: {'$in': comparable}}):
            key = getattr(doc, field)
            if key not in hash_docs:
                hash_docs[key] = doc

        for cmp_item in comparable:
            value = hash_docs.get(cmp_item)

            if not_found is NotFoundBehavior.NONE:
                yield value

            elif not_found is NotFoundBehavior.SKIP:
                if value is not None:
                    yield value

            elif not_found is NotFoundBehavior.ERROR:
                if value is not None:
                    yield value
                else:
                    raise NotFoundError("Could not find a document with"
                                        " the field '{}' equal '{}'"
                                        "".format(field, cmp_item))
