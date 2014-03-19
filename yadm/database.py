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
        return self._get_collection(document).insert(to_mongo(document))

    def save(self, document, upsert=False):
        if hasattr(document, '_id'):
            return self._get_collection(document).update(
                {'_id': document.id},
                {'$set': to_mongo(document, exclude=['_id'])},
                upsert=upsert,
                multi=False,
            )
        else:
            return self.insert(document)
