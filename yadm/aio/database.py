import pymongo

import yadm.abc as abc
from yadm.database import BaseDatabase
from yadm.serialize import to_mongo

from .queryset import AioQuerySet

PYMONGO_VERSION = pymongo.version_tuple


@abc.Database.register
class AioDatabase(BaseDatabase):
    def _get_collection(self, document_class, *, read_preference=None):
        # Return pymongo collection for document class.
        if PYMONGO_VERSION < (3, 0):
            collection = self.db[document_class.__collection__]

            if read_preference is not None:
                collection.read_preference = read_preference

            return collection

        else:
            return self.db.get_collection(document_class.__collection__,
                                          read_preference=read_preference)

    async def insert(self, document):
        document.__db__ = self
        collection = self._get_collection(document.__class__)
        document._id = await collection.insert(to_mongo(document))
        document.__changed_clear__()
        return document

    async def save(self, document):
        document.__db__ = self
        collection = self._get_collection(document.__class__)
        document._id = await collection.save(to_mongo(document))
        document.__changed_clear__()
        return document

    async def update_one(self, document, reload=True, *,
                         set=None, unset=None, inc=None,
                         push=None, pull=None):
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
            await self._get_collection(document).update(
                {'_id': document.id},
                update_data,
                upsert=False,
                multi=False,
            )

        if reload:
            await self.reload(document)

    async def remove(self, document):
        collection = self._get_collection(document.__class__)
        return await collection.remove({'_id': document._id})

    async def reload(self, document, new_instance=False):
        """ Reload document.

        :param Document document: instance for reload
        :param bool new_instance: if `True` return new instance of document,
            else change data in given document (default: `False`)
        """
        new = await self.get_queryset(document.__class__).find_one(document.id)

        if new_instance:
            return new
        else:
            document.__raw__.clear()
            document.__raw__.update(new.__raw__)
            document.__cache__.clear()
            document.__changed__.clear()
            return document

    def get_queryset(self, document_class, *, cache=None):
        return AioQuerySet(self, document_class, cache=cache)

    def aggregate(self, document_class, *, pipeline=None):
        return AioAggregator(self, document_class, pipeline=None)

    def bulk(self, document_class, ordered=False, raise_on_errors=True):
        return AioBulk(self, document_class, ordered, raise_on_errors)
