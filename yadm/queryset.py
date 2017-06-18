from enum import Enum
from bson import ObjectId

from yadm.join import Join
from yadm.cache import StackCache
from yadm.results import UpdateResult, RemoveResult
from yadm.serialize import from_mongo

CACHE_SIZE = 100


class NotFoundBehavior(Enum):
    NONE = 'none'
    SKIP = 'skip'
    ERROR = 'error'


class NotFoundError(Exception):
    pass


class BaseQuerySet:
    """ Query builder.

    :param db:
    :param document_class:
    :param cache:
    :param dict criteria:
    :param dict projection:
    :param list sort:
    :param slice slice:
    :param dict collection_params:
    """
    def __init__(self, db, document_class, *,
                 cache=None, criteria=None, projection=None, sort=None,
                 slice=None, collection_params=None):

        self._db = db
        self._document_class = document_class
        self._cache = cache
        self._criteria = {} if criteria is None else criteria
        self._projection = projection
        self._sort = sort
        self._slice = slice
        self._collection_params = collection_params or {}

    def __repr__(self):
        return ("{s.__class__.__name__}({s._document_class.__collection__}"
                " {s._criteria!r} {s._projection!r} {s._sort!r})"
                "".format(s=self))

    def __len__(self):
        return self.count()

    def __call__(self, criteria=None, projection=None):
        return self.find(criteria, projection)

    def __getitem__(self, item):
        if isinstance(item, slice):
            if item.step is not None:
                n = self.__class__.__name__
                raise TypeError("{} not support slicing with step".format(n))

            elif (item.start and item.start < 0) or (item.stop and item.stop < 0):
                n = self.__class__.__name__
                raise TypeError("{} not support negative slicing".format(n))

            return self.copy(slice=item)

        elif isinstance(item, int):
            return self._get_one(item)

        else:
            raise TypeError("Only slice or int accepted, but {}"
                            "".format(item.__class__))

    def _from_mongo_one(self, data, *, projection=None):
        """ Create document from raw data.
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
        doc.__qs__ = self
        return doc

    @property
    def _collection(self):  # noqa
        """ pymongo collection.
        """
        return self._db._get_collection(self._document_class,
                                        params=self._collection_params)

    @property
    def _cursor(self):
        """ Raw cursor with parameters from queryset.
        """
        cursor = self._collection.find(self._criteria, self._projection or None)

        if self._sort is not None:
            cursor = cursor.sort(self._sort)

        if self._slice is not None:
            if self._slice.start:
                cursor = cursor.skip(self._slice.start)

            if self._slice.stop:
                cursor = cursor.limit(self._slice.stop - (self._slice.start or 0))

        return cursor

    @property
    def cache(self):
        """ Queryset cache object.
        """
        if self._cache is None:
            self._cache = StackCache(size=CACHE_SIZE)

        return self._cache

    def copy(self, *, cache=None, criteria=None, projection=None,
             sort=None, slice=None, collection_params=None):
        """ Copy queryset with new parameters.

        Only keywords arguments is alowed.
        Parameters simply replaced with given arguments.

        :param cache:
        :param dict criteria:
        :param dict projection:
        :param list sort:
        :param slice slice:
        :param dict collection_params:

        :return: new :class:`yadm.queryset.QuerySet` object
        """
        return self.__class__(
            self._db, self._document_class,
            cache=cache or self._cache,
            criteria=criteria or self._criteria,
            projection=projection or self._projection,
            sort=sort or self._sort,
            slice=slice or self._slice,
            collection_params=collection_params or self._collection_params,
        )

    def read_preference(self, read_preference):
        """ Setup readPreference.

        Return new QuerySet instance.

        Deprecated since 1.4.0.
        Use `collection_params` argument in `copy`.
        """
        collection_params = (self._collection_params or {}).copy()
        collection_params['read_preference'] = read_preference
        return self.copy(collection_params=collection_params)

    def find(self, criteria=None, projection=None):
        """ Return queryset copy with new criteria and projection.

        :param dict criteria: update queryset's criteria
        :param dict projection: update queryset's projection
        :return: new :class:`yadm.queryset.QuerySet`

        .. code:: python

            qs({'field': {'$gt': 3}}, {'field': True})
        """
        if criteria is not None:
            if not self._criteria:
                criteria_new = criteria
            elif set(self._criteria) & set(criteria):
                criteria_new = {'$and': [self._criteria, criteria]}
            else:
                criteria_new = self._criteria.copy()
                criteria_new.update(criteria)
        else:
            criteria_new = None

        if projection is not None:
            if self._projection is None:
                projection_new = projection
            else:
                projection_new = self._projection.copy()
                criteria_new.update(projection)
        else:
            projection_new = None

        return self.copy(criteria=criteria_new, projection=projection_new)

    def fields(self, *fields):
        """ Get only setted fields.

        Update projection with fields.

        :param str fields:
        :return: new :class:`yadm.queryset.QuerySet`

        .. code:: python

            qs('field', 'field2')
        """
        return self.find(projection=dict.fromkeys(fields, True))

    def fields_all(self):
        """ Clear projection.
        """
        qs = self.copy()
        qs._projection = None
        return qs

    def sort(self, *sort):
        """ Return queryset with sorting.

        :param tuples sort: tuples with two items:
            `('field_name', sort_order_as_int)`.

        .. code:: python

            qs.sort(('field_1', 1), ('field_2', -1))
        """
        sort = list(sort)

        if self._sort is None:
            return self.copy(sort=sort)
        else:
            return self.copy(sort=self._sort + sort)

    def __iter__(self):
        raise NotImplementedError

    def __contains__(self, document):
        raise NotImplementedError

    def find_one(self, criteria=None, projection=None, *, exc=None):
        raise NotImplementedError

    def update(self, update, *, multi=True, upsert=False):
        raise NotImplementedError

    def find_and_modify(
            self, update=None, *, upsert=False,
            full_response=False, new=False,
            **kwargs):
        raise NotImplementedError

    def remove(self, *, multi=True):
        raise NotImplementedError

    def distinct(self, field):
        raise NotImplementedError

    def count(self):
        raise NotImplementedError

    def ids(self):
        raise NotImplementedError

    def join(self, *field_names):
        raise NotImplementedError


class QuerySet(BaseQuerySet):
    def __iter__(self):
        return self._from_mongo_list(self._cursor)

    def __contains__(self, document):
        return self.find_one(document.id) is not None

    def _get_one(self, index):
        return self._from_mongo_one(self._cursor[index])

    def _from_mongo_list(self, data):
        """ Generator for got documents from raw data list (cursor).
        """
        for d in data:
            yield self._from_mongo_one(d)

    def find_one(self, criteria=None, projection=None, *, exc=None):
        """ Find and return only one document.

        :param dict criteria: update queryset's criteria
        :param dict projection: update queryset's projection
        :param Exception exc: raise given exception if not found
        :return: :class:`yadm.documents.Document` or **None**

        .. code:: python

            qs({'field': {'$gt': 3}}, {'field': True})
        """
        if isinstance(criteria, ObjectId):
            criteria = {'_id': criteria}

        qs = self.find(criteria=criteria, projection=projection)
        data = self._collection.find_one(qs._criteria, qs._projection)

        if data is None:
            if exc is not None:
                raise exc(qs)
            else:
                return None

        return self._from_mongo_one(data, projection=qs._projection)

    def with_id(self, _id):
        """ Find document with id.

        This method is deprecated. Use find_one.

        :param _id: id of searching document
        :return: :class:`yadm.documents.Document` or **None**
        """
        import warnings
        warnings.warn("QuerySet.with_id is deprecated", DeprecationWarning)
        doc = self._document_class()
        doc._id = _id
        return self.find_one({'_id': doc._id})

    def update(self, update, *, multi=True, upsert=False):
        """ Update documents in queryset.

        :param dict update: update query
        :param bool multi: update all matched documents
            *(default True)*
        :param bool upsert: insert if not found
            *(default False)*
        :return: update result
        """
        raw_result = self._collection.update(
            self._criteria,
            update,
            multi=multi,
            upsert=upsert,
        )
        return UpdateResult(raw_result)

    def find_and_modify(
            self, update=None, *, upsert=False,
            full_response=False, new=False,
            **kwargs):
        """ Execute *$findAndModify* query.

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
            sort=self._sort or [],
            full_response=full_response,
            new=new,
            **kwargs
        )
        if not full_response:
            return self._from_mongo_one(result)
        else:
            result['value'] = self._from_mongo_one(result['value'])
            return result

    def remove(self, *, multi=True):
        """ Remove documents in queryset.

        :param bool multi: if False, remove only first finded document
            *(default True)*
        """
        raw_result = self._collection.remove(self._criteria, multi=multi)
        return RemoveResult(raw_result)

    def distinct(self, field):
        """ Distinct query.

        :param str field: field for distinct
        :return: list with result data
        """
        return self._cursor.distinct(field)

    def count(self):
        """ Count documents in queryset.

        :return: **int**
        """
        return self._cursor.count()

    def ids(self):
        """ Return all objects ids from queryset.
        """
        for raw in self.copy(projection={'_id': True})._cursor:
            yield raw['_id']

    def bulk(self):
        """ Return map {id: object}.

        :return: **dict**
        """
        qs = self.copy()
        qs._sort = None
        return {obj.id: obj for obj in qs}

    def join(self, *field_names):
        """ Create `yadm.Join` object, join `field_names` and return it.

        :param str fields_names: fields for join
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

    def find_in(self, comparable, field='_id', *,
                not_found=NotFoundBehavior.SKIP):
        """ Build ordered $in-query.

        Creates a query of the form {field: {'$in': comparable}} and
        returns the generator of documents with the same order as an elements
        in the argument 'comparable'.

        :param list comparable: values for compare in a query
        :param str field: field name of the document for comparison
        :param not_found: flag determines the behavior if the document
            with the specified value is not found
        :return: generator of docs

        not_found argument can take the following values:
            'none': If a document can not be found then a generator
                will return `None`.
            'skip': if a document can not be found then a generator
                will pass element.
            'error': if a document can not be found then a generator
                raise :class:`yadm.queryset.DocNotFoundError` exception.
        """
        not_found = NotFoundBehavior(not_found)
        hash_docs = {}

        for doc in self.find({field: {'$in': comparable}}):
            key = getattr(doc, field)
            if key not in hash_docs:
                hash_docs[key] = doc

        for cmp_item in comparable:
            value = hash_docs.get(cmp_item)

            if not_found is NotFoundBehavior.NONE:
                yield value

            elif not_found is NotFoundBehavior.SKIP:
                if value is not None:
                    yield value

            elif not_found is NotFoundBehavior.ERROR:
                if value is not None:
                    yield value
                else:
                    raise NotFoundError("Could not find a document with"
                                        " the field '{}' equal '{}'"
                                        "".format(field, cmp_item))
