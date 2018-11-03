from pymongo import ReturnDocument
from bson import ObjectId

from yadm.queryset import BaseQuerySet, NotFoundBehavior, NotFoundError
from yadm.serialize import to_mongo


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

    async def update_one(self, update, *, upsert=False):
        return await self._collection.update_one(
            self._criteria,
            update,
            upsert=upsert,
        )

    async def update_many(self, update, *, upsert=False):
        return await self._collection.update_many(
            self._criteria,
            update,
            upsert=upsert,
        )

    async def delete_one(self):
        return await self._collection.delete_one(self._criteria)

    async def delete_many(self):
        return await self._collection.delete_many(self._criteria)

    async def find_one_and_update(self, update, *,
                                  upsert=False,
                                  return_document=ReturnDocument.BEFORE):
        """ Find a single document and update it.
        """
        data = await self._collection.find_one_and_update(
            filter=self._criteria,
            projection=self._projection,
            update=update,
            upsert=upsert,
            sort=self._sort,
            return_document=return_document,
        )
        if data is None:  # pragma: no cover
            return None

        return self._from_mongo_one(data, projection=self._projection)

    async def find_one_and_replace(self, document, *,
                                   return_document=ReturnDocument.BEFORE):
        """ Find a single document and replace it.
        """
        data = await self._collection.find_one_and_replace(
            filter=self._criteria,
            projection=self._projection,
            replacement=to_mongo(document),
            sort=self._sort,
            return_document=return_document,
        )
        if data is None:  # pragma: no cover
            return None

        return self._from_mongo_one(data, projection=self._projection)

    async def find_one_and_delete(self):
        """ Find a single document and delete it.
        """
        data = await self._collection.find_one_and_delete(
            filter=self._criteria,
            projection=self._projection,
            sort=self._sort,
        )
        if data is None:  # pragma: no cover
            return None

        return self._from_mongo_one(data, projection=self._projection)

    async def count_documents(self) -> int:
        kwargs = {}
        if self._hint is not None:
            kwargs['hint'] = self._hint

        return await self._collection.count_documents(self._criteria, **kwargs)

    async def distinct(self, field):
        return await self._cursor.distinct(field)

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
