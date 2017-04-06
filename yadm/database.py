"""
This module for provide work with MongoDB database.

.. code-block:: python

    import pymongo
    from yadm.database import Database

    from mydocs import Doc

    client = pymongo.MongoClient("localhost", 27017)
    db = Database(self.client, 'test')

    doc = Doc()
    db.insert(doc)

    doc.arg = 13
    db.save(doc)

    qs = db.get_queryset(Doc).find({'arg': {'$gt': 10}})
    for doc in qs:
        print(doc)
"""
import pymongo

from yadm.markers import AttributeNotSet
from yadm.aggregation import Aggregator
from yadm.queryset import QuerySet
from yadm.bulk import Bulk
from yadm.serialize import to_mongo, from_mongo
from yadm.common import build_update_query


PYMONGO_VERSION = pymongo.version_tuple

RPS = pymongo.read_preferences


class BaseDatabase:
    def __init__(self, client, name, **database_params):
        self.client = client
        self.name = name
        self.database_params = database_params
        self.db = client.get_database(name, **database_params)

    def __repr__(self):  # pragma: no cover
        return '{}({!r})'.format(self.__class__.__name__, self.db)

    def __call__(self, document_class, **params):
        return self.get_queryset(document_class, **params)

    def _get_collection(self, document_class, params=None):
        """ Return pymongo collection for document class.
        """
        return self.db.get_collection(document_class.__collection__,
                                      **(params or {}))

    def insert(self, document, **collection_params):
        raise NotImplementedError

    def save(self, document, full=False, upsert=False, **collection_params):
        raise NotImplementedError

    def update_one(self, document, *, reload=True,
                   set=None, unset=None, inc=None,
                   push=None, pull=None,
                   **collection_params):
        raise NotImplementedError

    def remove(self, document, **collection_params):
        raise NotImplementedError

    def reload(self, document, new_instance=False, **collection_params):
        raise NotImplementedError

    def get_queryset(self, document_class, *,
                     cache=None, **collection_params):
        raise NotImplementedError

    def get_document(self, document_class, _id, *,
                     exc=None,
                     read_preference=RPS.PrimaryPreferred(),
                     **collection_params):
        raise NotImplementedError

    def aggregate(self, document_class, *,
                  pipeline=None, **collection_params):
        raise NotImplementedError

    def bulk(self, document_class, *,
             ordered=False, raise_on_errors=True, **collection_params):
        raise NotImplementedError


class Database(BaseDatabase):
    """ Main object who provide work with database.

    :param pymongo.Client client: database connection
    :param str name: database name
    """

    def insert(self, document, **collection_params):
        """ Insert document to database.

        :param Document document: document instance for insert to database

        It's bind new document to database set
        :py:attr:`_id <yadm.documents.Document._id>`.
        :param **collection_params: params for get_collection
        """
        document.__db__ = self
        collection = self._get_collection(document.__class__, collection_params)

        document._id = collection.insert_one(to_mongo(document)).inserted_id
        document.__changed_clear__()
        return document

    def save(self, document, full=False, upsert=False, **collection_params):
        """ Save document to database.

        :param Document document: document instance for save
        :param bool full: fully resave document
            (default: `False`)
        :param bool upsert: see documentation for MongoDB's `update`
            (default: `False`)
        :param **collection_params: params for get_collection

        If document has no `_id`
        :py:meth:`insert <Database.insert>` new document.
        """
        if hasattr(document, '_id'):
            document.__db__ = self

            if full:
                self._get_collection(document, collection_params).update(
                    {'_id': document.id},
                    to_mongo(document),
                    upsert=upsert,
                    multi=False,
                )
                document.__changed_clear__()
            else:
                set_data = to_mongo(
                    document,
                    exclude=['_id'],
                    include=list(document.__changed__),
                )

                unset_data = [f for f, v in document.__changed__.items()
                              if v is AttributeNotSet]

                self.update_one(document, set=set_data, unset=unset_data,
                                **collection_params)

            return document
        else:
            return self.insert(document, **collection_params)

    def update_one(self, document, *, reload=True,
                   set=None, unset=None, inc=None,
                   push=None, pull=None,
                   **collection_params):  # TODO: extend
        """ Update one document.

        :param Document document: document instance for update
        :param bool reload: if True, reload document
        :param **collection_params: params for get_collection
        """
        update_data = build_update_query(set=set, unset=unset, inc=inc,
                                         push=push, pull=pull)

        if update_data:
            self._get_collection(document, collection_params).update(
                {'_id': document.id},
                update_data,
                upsert=False,
                multi=False,
            )

        if reload:
            self.reload(document, **collection_params)

    def remove(self, document, **collection_params):
        """ Remove document from database.

        :param Document document: instance for remove from database
        :param **collection_params: params for get_collection
        """
        col = self._get_collection(document.__class__, collection_params)
        return col.remove({'_id': document._id})

    def reload(self, document, new_instance=False,
               read_preference=RPS.PrimaryPreferred(),
               **collection_params):
        """ Reload document.

        :param Document document: instance for reload
        :param bool new_instance: if `True` return new instance of document,
            else change data in given document (default: `False`)
        :param **collection_params: params for get_collection
        """
        collection_params['read_preference'] = read_preference
        qs = self.get_queryset(document.__class__, **collection_params)
        new = qs.find_one(document.id)

        if new_instance:
            return new
        else:
            document.__raw__.clear()
            document.__raw__.update(new.__raw__)
            document.__cache__.clear()
            document.__changed__.clear()
            return document

    def get_queryset(self, document_class, *,
                     cache=None,
                     **collection_params):
        """ Return queryset for document class.

        :param document_class: :class:`yadm.documents.Document`
        :param cache: cache for share with other querysets
        :param **collection_params: params for get_collection

        This create instance of :class:`yadm.queryset.QuerySet`
        with presetted document's collection information.
        """
        return QuerySet(self, document_class, cache=cache,
                        collection_params=collection_params)

    def get_document(self, document_class, _id, *,
                     exc=None,
                     read_preference=RPS.PrimaryPreferred(),
                     **collection_params):
        """ Get document for it _id.

        :param document_class: :class:`yadm.documents.Document`
        :param _id: document's _id
        :param Exception exc: raise given exception if not found
        :param **collection_params: params for get_collection

        Default ReadPreference is PrimaryPreferred.
        """
        collection_params['read_preference'] = read_preference
        col = self.db.get_collection(document_class.__collection__,
                                     **collection_params)

        raw = col.find_one({'_id': _id})

        if raw:
            doc = from_mongo(document_class, raw)
            doc.__db__ = self
            return doc

        elif exc is not None:
            raise exc((document_class, _id, collection_params))

        else:
            return None

    def aggregate(self, document_class, *,
                  pipeline=None,
                  **collection_params):
        """ Return aggregator for use aggregation framework.

        :param document_class: :class:`yadm.documents.Document`
        :param list pipeline: initial pipeline
        :param **collection_params: params for get_collection
        """
        return Aggregator(self, document_class, pipeline=None,
                          collection_params=collection_params)

    def bulk(self, document_class, *,
             ordered=False, raise_on_errors=True,
             **collection_params):
        """ Return Bulk.

        :param MetaDocument document_class: class of documents fo bulk
        :param bool ordered: create ordered bulk (default `False`)
        :param bool raise_on_errors: raise BulkWriteError exception
            if write errors (default `True`)
        :param **collection_params: params for get_collection

        Context manager:

            with db.bulk(Doc) as bulk:
                bulk.insert(doc_1)
                bulk.insert(doc_2)
        """
        return Bulk(self, document_class, ordered,
                    raise_on_errors, collection_params)
