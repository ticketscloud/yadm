from yadm.queryset import QuerySet
from yadm.serialize import to_mongo


class Database:
    def __init__(self, client, name):
        self.client = client
        self.name = name
        self.db = client[name]

    def __repr__(self):
        return 'Database({!r})'.format(self.db)

    def __call__(self, *args, **kwargs):
        return self.get_queryset(*args, **kwargs)

    def _get_collection(self, document):
        return self.db[document.__collection__]

    def get_queryset(self, document):
        return QuerySet(self, document)

    def insert(self, document):
        ret = self._get_collection(document).insert(to_mongo(document))
        document.__fields_changed__.clear()
        document.__db__ = self
        return ret

    def save(self, document, upsert=False):
        if hasattr(document, '_id'):
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
            document.__db__ = self
            return ret
        else:
            return self.insert(document)
