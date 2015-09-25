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
from yadm.markers import AttributeNotSet
from yadm.queryset import QuerySet
from yadm.bulk import Bulk
from yadm.serialize import to_mongo


class Database:
    """ Main object who provide work with database

    :param pymongo.Client client: database connection
    :param str name: database name
    """
    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.db = client[name]

    def __repr__(self):
        return 'Database({!r})'.format(self.db)

    def __call__(self, *args, **kwargs):
        return self.get_queryset(*args, **kwargs)

    def _get_collection(self, document_class):
        """ Return pymongo collection for document class
        """
        return self.db[document_class.__collection__]

    def get_queryset(self, document_class):
        """ Return queryset for document class

        :param document_class: :class:`yadm.documents.Document`

        This create instance of :class:`yadm.queryset.QuerySet`
        with presetted document's collection information.
        """
        return QuerySet(self, document_class)

    def insert(self, document):
        """ Insert document to database

        :param Document document: document instance for insert to database

        It's set :attr:`yadm.documents.Document._id`.
        """
        document.__db__ = self
        collection = self._get_collection(document.__class__)
        document._id = collection.insert(to_mongo(document))
        document.__changed_clear__()
        return document

    def save(self, document, full=False, upsert=False):
        """ Save document to database

        :param Document document: document instance for save
        :param bool full: fully resave document
            (default: `False`)
        :param bool upsert: see documentation for MongoDB's `update`
            (default: `False`)

        If document has not `id` this :meth:`insert` new document.
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
        """ Update one document

        :param Document document: document instance for update
        :param bool reload: if True, reload document
        """
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
            self._get_collection(document).update(
                {'_id': document.id},
                update_data,
                upsert=False,
                multi=False,
            )

        if reload:
            self.reload(document)

    def remove(self, document):
        """ Remove document from database

        :param Document document: document instance for remove from database
        """
        return self._get_collection(document.__class__).remove({'_id': document._id})

    def reload(self, document, new_instance=False):
        """ Reload document

        :param Document document: document for reload
        :param bool new_instance: if `True` return new instance of document,
            else change data in given document (default: `False`)
        """
        new = self.get_queryset(document.__class__).with_id(document.id)

        if new_instance:
            return new
        else:
            document.__raw__.clear()
            document.__raw__.update(new.__raw__)
            document.__cache__.clear()
            document.__changed__.clear()
            return document

    def bulk(self, document_class, ordered=False, raise_on_errors=True):
        """ Return Bulk

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
