from yadm.queryset import QuerySet
from yadm.serialize import to_mongo


class Database:
    """ Main object for work with database
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
        """
        return QuerySet(self, document_class)

    def insert(self, document):
        """ Insert document to database
        """
        document.__db__ = self
        document._id = self._get_collection(document).insert(to_mongo(document))
        document.__fields_changed__.clear()
        return document

    def save(self, document, upsert=False):
        """ Save document to database
        """
        if hasattr(document, '_id'):
            document.__db__ = self

            ret = self._get_collection(document).update(
                {'_id': document.id},
                {'$set': to_mongo(
                    document,
                    exclude=['_id'],
                    include=document.__fields_changed__),
                },
                upsert=upsert,
                multi=False,
            )
            document.__fields_changed__.clear()
            return document
        else:
            return self.insert(document)
