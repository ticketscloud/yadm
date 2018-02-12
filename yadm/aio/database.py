import itertools

import pymongo

import yadm.abc as abc
from yadm.database import BaseDatabase
from yadm.serialize import to_mongo, from_mongo
from yadm.bulk_writer import BATCH_SIZE as BULK_BATCH_SIZE

from .queryset import AioQuerySet
from .aggregation import AioAggregator
from .bulk_writer import AioBulkWriter

PYMONGO_VERSION = pymongo.version_tuple

RPS = pymongo.read_preferences


@abc.Database.register
class AioDatabase(BaseDatabase):
    aio = True

    async def insert_one(self, document, **collection_params):
        document.__db__ = self
        collection = self._get_collection(document.__class__, collection_params)
        result = await collection.insert_one(to_mongo(document))
        document._id = result.inserted_id
        document.__changed_clear__()
        return result

    async def insert_many(self, documents, *, ordered=True, **collection_params):
        if ordered:
            documents = list(documents)
            if documents:
                collection = self._get_collection(documents[0].__class__,
                                                  collection_params)

                _gen = (to_mongo(doc) for doc in documents)
                result = await collection.insert_many(_gen, ordered=ordered)

                for _id, document in zip(result.inserted_ids, documents):
                    document.__db__ = self
                    document.id = _id

                return result
            else:
                return pymongo.results.InsertManyResult([], True)

        else:
            iterator = iter(documents)
            first = next(iterator)
            collection = self._get_collection(first.__class__,
                                              collection_params)

            _gen = (to_mongo(doc) for doc in itertools.chain([first], iterator))
            return await collection.insert_many(_gen, ordered=ordered)

    async def save(self, document, **collection_params):
        document.__db__ = self
        collection = self._get_collection(document.__class__, collection_params)
        document._id = await collection.save(to_mongo(document))
        document.__changed_clear__()
        return document

    async def update_one(self, document, reload=True, *,
                         set=None, unset=None, inc=None,
                         push=None, pull=None,
                         **collection_params):
        update_data = {}

        if set:
            update_data['$set'] = set

        if unset:
            if isinstance(unset, dict):
                update_data['$unset'] = unset
            else:
                update_data['$unset'] = {f: True for f in unset}

        if inc:
            update_data['$inc'] = inc

        if push:
            update_data['$push'] = push

        if pull:
            update_data['$pull'] = pull

        if update_data:
            await self._get_collection(document, collection_params).update(
                {'_id': document.id},
                update_data,
                upsert=False,
                multi=False,
            )

        if reload:
            await self.reload(document)

    async def delete_one(self, document, **collection_params):
        _col = self._get_collection(document.__class__, collection_params)
        return await _col.delete_one({'_id': document._id})

    async def reload(self, document, new_instance=False, *,
                     projection=None,
                     read_preference=RPS.PrimaryPreferred(),
                     **collection_params):
        collection_params['read_preference'] = read_preference

        qs = self.get_queryset(document.__class__,
                               projection=projection,
                               **collection_params)

        new = await qs.find_one(document.id)

        if new_instance:
            return new
        else:
            document.__raw__.clear()
            document.__raw__.update(new.__raw__)
            document.__cache__.clear()
            document.__changed__.clear()
            return document

    async def get_document(self, document_class, _id, *,
                           exc=None,
                           read_preference=RPS.PrimaryPreferred(),
                           **collection_params):
        collection_params['read_preference'] = read_preference
        col = self.db.get_collection(document_class.__collection__,
                                     **collection_params)

        raw = await col.find_one({'_id': _id})

        if raw:
            doc = from_mongo(document_class, raw)
            doc.__db__ = self
            return doc

        elif exc is not None:
            raise exc((document_class, _id, collection_params))

        else:
            return None

    def get_queryset(self, document_class, *,
                     projection=None,
                     cache=None,
                     **collection_params):
        if projection is None:
            projection = document_class.__default_projection__

        return AioQuerySet(self, document_class,
                           projection=projection,
                           cache=cache,
                           collection_params=collection_params)

    def aggregate(self, document_class, *, pipeline=None, **collection_params):
        return AioAggregator(self, document_class,
                             pipeline=pipeline,
                             collection_params=collection_params)

    def bulk_write(self, document_class, *,
                   ordered=False,
                   batch_size=BULK_BATCH_SIZE,
                   **collection_params):
        """ Return AioBulkWriter for realize bulk_write from pymongo.
        """
        return AioBulkWriter(self, document_class,
                             ordered=ordered, batch_size=batch_size,
                             collection_params=collection_params)
