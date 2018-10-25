import itertools

import pymongo
from bson import ObjectId

from yadm.log_items import Insert, Save, UpdateOne, DeleteOne, Reload
from yadm.database import BaseDatabase
from yadm.serialize import to_mongo, from_mongo
from yadm.bulk_writer import BATCH_SIZE as BULK_BATCH_SIZE
from yadm.common import build_update_query

from .queryset import AioQuerySet
from .aggregation import AioAggregator
from .bulk_writer import AioBulkWriter

RPS = pymongo.read_preferences


class AioDatabase(BaseDatabase):
    aio = True

    async def insert_one(self, document, **collection_params):
        document.__db__ = self
        collection = self._get_collection(document.__class__,
                                          collection_params)

        result = await collection.insert_one(to_mongo(document))

        document._id = result.inserted_id
        document.__log__.append(Insert(id=result.inserted_id))
        return result

    async def insert_many(self, documents, *, ordered=True, **collection_params):
        def gen(documents):
            for document in documents:
                yield to_mongo(document)
                document.__log__.append(Insert())

        # TODO: rewrite this!
        if ordered:
            documents = list(documents)
            if documents:
                collection = self._get_collection(documents[0].__class__,
                                                  collection_params)

                result = await collection.insert_many(
                    gen(documents),
                    ordered=True,
                )

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

            return await collection.insert_many(
                gen(itertools.chain([first], iterator)),
                ordered=False,
            )

    async def save(self, document, **collection_params):
        document.__db__ = self
        if not hasattr(document, 'id'):
            document.id = ObjectId()

        raw = to_mongo(document)
        collection = self._get_collection(document, collection_params)
        await collection.find_one_and_replace(
            filter={'_id': document.id},
            replacement=raw,
            return_document=pymongo.collection.ReturnDocument.AFTER,
            upsert=True,
        )
        document.__log__.append(Save(id=document.id))
        return document

    async def update_one(self, document, *, reload=True,
                         set=None, unset=None, inc=None,
                         push=None, pull=None,
                         **collection_params):  # TODO: extend
        update_data = build_update_query(set=set, unset=unset, inc=inc,
                                         push=push, pull=pull)

        if update_data:
            collection = self._get_collection(document, collection_params)
            result = await collection.update_one(
                {'_id': document.id},
                update_data,
                upsert=False,
            )
            document.__log__.append(UpdateOne(update_data=update_data))
        else:
            result = None

        if reload:
            await self.reload(document, **collection_params)

        return result

    async def delete_one(self, document, **collection_params):
        collection = self._get_collection(document.__class__, collection_params)
        res = await collection.delete_one({'_id': document._id})
        document.__log__.append(DeleteOne())
        return res

    async def reload(self, document, new_instance=False, *,
                     projection=None,
                     read_preference=RPS.PrimaryPreferred(),
                     **collection_params):
        collection_params['read_preference'] = read_preference
        qs = self.get_queryset(document.__class__,
                               projection=projection,
                               **collection_params)

        if projection is not None:
            new = await qs.find_one(document.id, projection)
        else:
            new = await qs.find_one(document.id)

        if new_instance:
            return new
        else:
            document.__raw__.clear()
            document.__raw__.update(new.__raw__)
            document.__cache__.clear()
            document.__log__.append(Reload())
            document.__not_loaded__ = new.__not_loaded__
            return document

    async def get_document(self, document_class, _id, *,
                           projection=None,
                           exc=None,
                           read_preference=RPS.PrimaryPreferred(),
                           **collection_params):
        collection_params['read_preference'] = read_preference
        col = self.db.get_collection(document_class.__collection__,
                                     **collection_params)

        if projection is None:
            projection = document_class.__default_projection__

        if projection is not None:
            raw = await col.find_one({'_id': _id}, projection)
            not_loaded = [k for k, v in projection.items() if not v]
        else:
            raw = await col.find_one({'_id': _id})
            not_loaded = []

        if raw:
            doc = from_mongo(document_class, raw, not_loaded=not_loaded)
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
