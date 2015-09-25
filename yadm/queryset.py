from yadm.join import Join
from yadm.serialize import from_mongo


class QuerySet:
    """ Query builder
    """
    def __init__(self, db, document_class):
        self._db = db
        self._document_class = document_class
        self._criteria = {}
        self._projection = None
        self._slice = None
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
            qs = self.copy()
            qs._slice = item
            return qs

        elif isinstance(item, int):
            return self._from_mongo_one(self._cursor[item])

        else:
            raise TypeError('Only slice or int accepted')

    def __call__(self, criteria=None, projection=None):
        return self.find(criteria, projection)

    def _from_mongo_one(self, data, projection=None):
        """ Create document from raw data
        """
        projection = projection or self._projection

        if data is None:
            return None
        elif not projection:
            not_loaded = ()
        else:
            include = [f for f, v in projection.items() if v]
            exclude = {f for f, v in projection.items() if not v}

            if include:
                if exclude and exclude != {'_id'}:
                    raise ValueError("projection cannot have a mix"
                                     " of inclusion and exclusion")

                for field_name in self._document_class.__fields__:
                    if field_name not in include:
                        exclude.add(field_name)

            not_loaded = exclude

        doc = from_mongo(self._document_class, data, not_loaded)
        doc.__db__ = self._db
        return doc

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

        if self._slice is not None:
            cursor = cursor[self._slice]

        return cursor

    def _copy_qs(self):
        """ Copy queryset instance
        """
        qs = self.__class__(self._db, self._document_class)
        qs._criteria = self._criteria.copy()

        if self._projection is not None:
            qs._projection = self._projection.copy()

        qs._sort = self._sort[:]

        return qs

    def _update_qs(self, criteria=None, projection=None, sort=None):
        """ Update queryset parameters in place
        """
        if criteria:
            if not self._criteria:
                self._criteria = criteria

            elif set(self._criteria) & set(criteria):
                self._criteria = {'$and': [self._criteria, criteria]}
            else:
                self._criteria.update(criteria)

        if projection:
            if self._projection is None:
                self._projection = projection
            else:
                self._projection.update(projection)

        if sort:
            self._sort.extend(sort)

    def copy(self, *args, **kwargs):
        """ Copy queryset and update it

        :param dict criteria:
        :param dict projection:
        :param dict sort:
        :return: new :class:`yadm.queryset.QuerySet` object
        """
        qs = self._copy_qs()
        qs._update_qs(*args, **kwargs)
        return qs

    def find(self, criteria=None, projection=None):
        """ Return queryset copy with new criteria and projection

        :param dict criteria: update queryset's criteria
        :param dict projection: update queryset's projection
        :return: new :class:`yadm.queryset.QuerySet`

        .. code:: python

            qs({'field': {'$gt': 3}}, {'field': True})
        """
        return self.copy(criteria=criteria, projection=projection)

    def find_one(self, criteria=None, projection=None):
        """ Find and return only one document

        :param dict criteria: update queryset's criteria
        :param dict projection: update queryset's projection
        :return: :class:`yadm.documents.Document` or **None**

        .. code:: python

            qs({'field': {'$gt': 3}}, {'field': True})
        """
        qs = self.copy(criteria=criteria, projection=projection)
        collection = qs._db._get_collection(qs._document_class)
        data = collection.find_one(qs._criteria, qs._projection)
        return self._from_mongo_one(data, qs._projection)

    def with_id(self, _id):
        """ Find document with id

        :param _id: id of searching document
        :return: :class:`yadm.documents.Document` or **None**
        """
        doc = self._document_class()
        doc._id = _id
        return self.find_one({'_id': doc._id})

    def update(self, update, multi=True):
        """ Update documents in queryset

        :param dict update: update query
        :param bool multi: update all matched documents
            *(default True)*
        :return: update result
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
        """ Execute *$findAndModify* query

        :param dict update: see second argument to update()
        :param bool upsert: insert if object doesn’t exist
            *(default False)*
        :param bool full_response: return the entire response
            object from the server *(default False)*
        :param new: return updated rather than original object
            *(default False)*
        :param kwargs: any other options the findAndModify
            command supports can be passed here
        :return: :class:`yadm.documents.Document` or **None**
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
        """ Remove documents in queryset
        """
        return self._collection.remove(self._criteria)

    def fields(self, *fields):
        """ Get only setted fields

        Update projection with fields.

        :param str fields:
        :return: new :class:`yadm.queryset.QuerySet`

        .. code:: python

            qs('field', 'field2')
        """
        return self.copy(projection=dict.fromkeys(fields, True))

    def fields_all(self):
        """ Clear projection
        """
        qs = self.copy()
        qs._projection = None
        return qs

    def sort(self, *sort):
        """ Sort query

        :param tuples sort: tuples with two items:
            `('field_name', sort_order_as_int)`.

        .. code:: python

            qs.sort(('field_1', 1), ('field_2', -1))
        """
        return self.copy(sort=sort)

    def count(self):
        """ Count documents in queryset

        :return: **int**
        """
        return self._cursor.count()

    def bulk(self):
        """ Return map {id: object}

        :return: **dict**
        """
        qs = self.copy()
        qs._sort = []
        return {obj.id: obj for obj in qs}

    def join(self, *field_names):
        """ Create `yadm.Join` object, join `field_names` and return it

        :param str fiels_names: fields for join
        :return: new :class:`yadm.join.Join`

        Next algorithm for join:
            1. Get all documents from queryset;
            2. Aggegate all ids from requested fields;
            3. Make *$in* queries for get joined documents;
            4. Bind joined documents to objects from first queryset;

        `Join` object is instance of `abc.Sequence`.
        """
        join = Join(self)

        if field_names:
            join.join(*field_names)

        return join
