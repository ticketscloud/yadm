from yadm.serialize import from_mongo


class QuerySet:
    def __init__(self, db, document_class):
        self._db = db
        self._document_class = document_class
        self._criteria = None
        self._projection = None
        self._sort = []

    def __str__(self):
        return ('QuerySet({s._document_class.__collection__},'
                ' {s._criteria!r}, {s._projection!r}, {s._sort!r})'.format(s=self))

    def __len__(self):
        return self.count()

    def __iter__(self):
        return self._from_mongo_list(self._cursor)

    def __contains__(self, document):
        return self.with_id(document.id) is not None

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self._from_mongo_list(self._cursor[item])

        elif isinstance(item, int):
            return self._from_mongo_one(self._cursor[item])

        else:
            raise TypeError('Only slice or int accepted')

    def __call__(self, criteria=None, projection=None):
        return self.find(criteria, projection)

    def _from_mongo_one(self, data):
        """ Create document from raw data
        """
        if data is not None:
            doc = from_mongo(self._document_class, data)
            doc.__db__ = self._db
            return doc
        else:
            return None

    def _from_mongo_list(self, data):
        """ Generator for got documents from raw data list (cursor)
        """
        for d in data:
            yield self._from_mongo_one(d)

    @property
    def _collection(self):
        """ pymongo collection
        """
        return self._db._get_collection(self._document_class)

    @property
    def _cursor(self):
        """ pymongo cursor with parameters from queryset
        """
        cursor = self._collection.find(self._criteria, self._projection or None)

        if self._sort:
            cursor = cursor.sort(self._sort)

        return cursor

    def _copy_qs(self):
        """ Copy queryset instance
        """
        qs = self.__class__(self._db, self._document_class)

        if self._criteria is not None:
            qs._criteria = self._criteria.copy()

        if self._projection is not None:
            qs._projection = self._projection.copy()

        qs._sort = self._sort[:]

        return qs

    def _update_qs(self, **kwargs):
        """ Update queryset parameters in place
        """
        if 'criteria' in kwargs:
            if self._criteria is None:
                self._criteria = kwargs['criteria']
            else:
                self._criteria.update(kwargs['criteria'] or {})

        if 'projection' in kwargs:
            if self._projection is None:
                self._projection = kwargs['projection']
            else:
                self._projection.update(kwargs['projection'] or {})

        if 'sort' in kwargs and kwargs['sort'] is not None:
            self._sort.extend(kwargs['sort'])

    def copy(self, **kwargs):
        """ Copy queryset and update it

            :criteria:
            :projection:
            :sort:
        """
        qs = self._copy_qs()
        qs._update_qs(**kwargs)
        return qs

    def find(self, criteria=None, projection=None):
        """ Return queryset copy with new criteria and projection

            qs({'field': {'$gt': 3}}, {'field': True})
        """
        return self.copy(criteria=criteria, projection=projection)

    def find_one(self, criteria=None, projection=None):
        """ Find and return only one document

            qs({'field': {'$gt': 3}}, {'field': True})
        """
        qs = self.copy(criteria=criteria, projection=projection)
        collection = qs._db._get_collection(qs._document_class)
        data = collection.find_one(qs._criteria, qs._projection)
        return self._from_mongo_one(data)

    def with_id(self, _id):
        """ Find document with id
        """
        doc = self._document_class()
        doc._id = _id
        return self.find_one({'_id': doc._id})

    def update(self, update, multi=True):
        """ Update documents in queryset
        """
        return self._collection.update(
            self._criteria,
            update,
            multi=multi,
            upsert=False,
        )

    def find_and_modify(
            self, update=None, upsert=False,
            full_response=False, new=False,
            **kwargs):
        """ Execute findAndModify query

        :param dict update: see second argument to update()
        :param bool upsert: insert if object doesn’t exist
            (default False)
        :param bool full_response: return the entire response
            object from the server (default False)
        :param new: return updated rather than original object
            (default False)
        :param **kwargs: any other options the findAndModify
            command supports can be passed here
        """
        result = self._collection.find_and_modify(
            query=self._criteria,
            update=update,
            upsert=upsert,
            sort=self._sort or None,
            full_response=full_response,
            new=new,
            **kwargs
        )
        if not full_response:
            return self._from_mongo_one(result)
        else:
            result['value'] = self._from_mongo_one(result['value'])
            return result

    def remove(self):
        """ Remove documents from queryset
        """
        return self._collection.remove(self._criteria)

    def fields(self, *fields):
        """ Get only setted fields

        Update projection with fields.

            qs(field, field2)
        """
        return self.copy(projection=dict.fromkeys(fields, True))

    def fields_all(self):
        """ Clear projection
        """
        qs = self.copy()
        qs._projection = None
        return qs

    def sort(self, *sort):
        """
            qs.sort(('field_1', 1), ('field_2', -1))
        """
        return self.copy(sort=sort)

    def count(self):
        """ Count documents in queryset
        """
        return self._cursor.count()

    def bulk(self):
        """ Return map {id: object}
        """
        qs = self.copy()
        qs._sort = []
        return {obj.id: obj for obj in qs}
