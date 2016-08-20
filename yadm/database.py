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
from yadm.serialize import to_mongo
from yadm.common import build_update_query


PYMONGO_VERSION = pymongo.version_tuple


class BaseDatabase:
    def __init__(self, client, name, *, read_preference=None):
        self.client = client
        self.name = name

        if PYMONGO_VERSION < (3, 0):
            self.db = client[name]

            if read_preference is not None:
                self.db.read_preference = read_preference

        else:
            self.db = client.get_database(name, read_preference=read_preference)

    def __repr__(self):  # pragma: no cover
        return '{}({!r})'.format(self.__class__.__name__, self.db)

    def __call__(self, document_class, *, cache=None):
        return self.get_queryset(document_class, cache=cache)

    def _get_collection(self, document_class, *, read_preference=None):
        """ Return pymongo collection for document class.
        """
        if PYMONGO_VERSION < (3, 0):
            collection = self.db[document_class.__collection__]

            if read_preference is not None:
                collection.read_preference = read_preference

            return collection

        else:
            return self.db.get_collection(document_class.__collection__,
                                          read_preference=read_preference)

    def insert(self, document):
        raise NotImplementedError

    def save(self, document, full=False, upsert=False):
        raise NotImplementedError

    def update_one(self, document, reload=True, *,
                   set=None, unset=None, inc=None,
                   push=None, pull=None):
        raise NotImplementedError

    def remove(self, document):
        raise NotImplementedError

    def reload(self, document, new_instance=False):
        raise NotImplementedError

    def get_queryset(self, document_class, *, cache=None):
        raise NotImplementedError

    def aggregate(self, document_class, *, pipeline=None):
        raise NotImplementedError

    def bulk(self, document_class, ordered=False, raise_on_errors=True):
        raise NotImplementedError


class Database(BaseDatabase):
    """ Main object who provide work with database.

    :param pymongo.Client client: database connection
    :param str name: database name
    """

    def insert(self, document):
        """ Insert document to database.

        :param Document document: document instance for insert to database

        It's bind new document to database set
        :py:attr:`_id <yadm.documents.Document._id>`.
        """
        document.__db__ = self
        collection = self._get_collection(document.__class__)
        document._id = collection.insert(to_mongo(document))
        document.__changed_clear__()
        return document

    def save(self, document, full=False, upsert=False):
        """ Save document to database.

        :param Document document: document instance for save
        :param bool full: fully resave document
            (default: `False`)
        :param bool upsert: see documentation for MongoDB's `update`
            (default: `False`)

        If document has no `_id`
        :py:meth:`insert <Database.insert>` new document.
        """
        if hasattr(document, '_id'):
            document.__db__ = self

            if full:
                self._get_collection(document).update(
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

                self.update_one(document, set=set_data, unset=unset_data)

            return document
        else:
            return self.insert(document)

    def update_one(self, document, reload=True, *,
                   set=None, unset=None, inc=None,
                   push=None, pull=None):  # TODO: extend
        """ Update one document.

        :param Document document: document instance for update
        :param bool reload: if True, reload document
        """
        update_data = build_update_query(set=set, unset=unset, inc=inc,
                                         push=push, pull=pull)

        if update_data:
            self._get_collection(document).update(
                {'_id': document.id},
                update_data,
                upsert=False,
                multi=False,
            )

        if reload:
            self.reload(document)

    def remove(self, document):
        """ Remove document from database.

        :param Document document: instance for remove from database
        """
        return self._get_collection(document.__class__).remove({'_id': document._id})

    def reload(self, document, new_instance=False):
        """ Reload document.

        :param Document document: instance for reload
        :param bool new_instance: if `True` return new instance of document,
            else change data in given document (default: `False`)
        """
        new = self.get_queryset(document.__class__).find_one(document.id)

        if new_instance:
            return new
        else:
            document.__raw__.clear()
            document.__raw__.update(new.__raw__)
            document.__cache__.clear()
            document.__changed__.clear()
            return document

    def get_queryset(self, document_class, *, cache=None):
        """ Return queryset for document class.

        :param document_class: :class:`yadm.documents.Document`
        :param cache: cache for share with other querysets

        This create instance of :class:`yadm.queryset.QuerySet`
        with presetted document's collection information.
        """
        return QuerySet(self, document_class, cache=cache)

    def aggregate(self, document_class, *, pipeline=None):
        """ Return aggregator for use aggregation framework.

        :param document_class: :class:`yadm.documents.Document`
        :param list pipeline: initial pipeline
        """
        return Aggregator(self, document_class, pipeline=None)

    def bulk(self, document_class, ordered=False, raise_on_errors=True):
        """ Return Bulk.

        :param MetaDocument document_class: class of documents fo bulk
        :param bool ordered: create ordered bulk (default `False`)
        :param bool raise_on_errors: raise BulkWriteError exception
            if write errors (default `True`)

        Context manager:

            with db.bulk(Doc) as bulk:
                bulk.insert(doc_1)
                bulk.insert(doc_2)
        """
        return Bulk(self, document_class, ordered, raise_on_errors)
