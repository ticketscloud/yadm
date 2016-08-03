import abc


class Database(metaclass=abc.ABCMeta):
    """ Database.

    :param pymongo.Client client: database connection
    :param str name: database name
    :param bool read_preference: setup read_preference
    """

    @abc.abstractmethod
    def __init__(self, client, name, *, read_preference=None):
        pass

    @abc.abstractmethod
    def insert(self, document):
        """ Insert document to database.

        :param Document document: document instance for insert to database

        It's bind new document to database set
        :py:attr:`_id <yadm.documents.Document._id>`.
        """

    @abc.abstractmethod
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

    @abc.abstractmethod
    def update_one(self, document, reload=True, *,
                   set=None, unset=None, inc=None,
                   push=None, pull=None):  # TODO: extend
        """ Update one document.

        :param Document document: document instance for update
        :param bool reload: if True, reload document
        """

    @abc.abstractmethod
    def remove(self, document):
        """ Remove document from database.

        :param Document document: instance for remove from database
        """

    @abc.abstractmethod
    def reload(self, document, new_instance=False):
        """ Reload document.

        :param Document document: instance for reload
        :param bool new_instance: if `True` return new instance of document,
            else change data in given document (default: `False`)
        """

    @abc.abstractmethod
    def get_queryset(self, document_class, *, cache=None):
        """ Return queryset for document class.

        :param document_class: :class:`yadm.documents.Document`
        :param cache: cache for share with other querysets

        This create instance of :class:`yadm.queryset.QuerySet`
        with presetted document's collection information.
        """

    @abc.abstractmethod
    def aggregate(self, document_class, *, pipeline=None):
        """ Return aggregator for use aggregation framework.

        :param document_class: :class:`yadm.documents.Document`
        :param list pipeline: initial pipeline
        """

    @abc.abstractmethod
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


class QuerySet(metaclass=abc.ABCMeta):
    """ Query builder.

    :param cache:
    :param dict criteria:
    :param dict projection:
    :param list sort:
    :param slice slice:
    :param int read_preference:
    """
    @abc.abstractmethod
    def __init__(self, db, document_class, *,
                 cache=None, criteria=None, projection=None, sort=None,
                 slice=None, read_preference=None):
        pass

    @abc.abstractmethod
    def __len__(self):
        return self.count()

    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def __contains__(self, document):
        pass

    @abc.abstractmethod
    def __getitem__(self, item):
        pass

    @abc.abstractmethod
    def __call__(self, criteria=None, projection=None):
        return self.find(criteria=None, projection=None)

    @property
    @abc.abstractmethod
    def cache(self):
        """ Queryset cache object.
        """

    @abc.abstractmethod
    def copy(self, *, cache=None, criteria=None, projection=None,
             sort=None, slice=None, read_preference=None):
        """ Copy queryset with new parameters.

        Only keywords arguments is alowed.
        Parameters simply replaced with given arguments.

        :param cache:
        :param dict criteria:
        :param dict projection:
        :param list sort:
        :param slice slice:
        :param int read_preference:

        :return: new :class:`yadm.queryset.QuerySet` object
        """

    @abc.abstractmethod
    def find(self, criteria=None, projection=None):
        """ Return queryset copy with new criteria and projection.

        :param dict criteria: update queryset's criteria
        :param dict projection: update queryset's projection
        :return: new :class:`yadm.queryset.QuerySet`

        .. code:: python

            qs.find({'field': {'$gt': 3}}, {'field': True})
        """

    @abc.abstractmethod
    def find_one(self, criteria=None, projection=None, *, exc=None):
        """ Find and return only one document.

        :param dict criteria: update queryset's criteria
        :param dict projection: update queryset's projection
        :param Exception exc: raise given exception if not found
        :return: :class:`yadm.documents.Document` or **None**

        .. code:: python

            qs.find_one({'field': {'$gt': 3}}, {'field': True})
        """

    @abc.abstractmethod
    def update(self, update, *, multi=True, upsert=False):
        """ Update documents in queryset.

        :param dict update: update query
        :param bool multi: update all matched documents
            *(default True)*
        :param bool upsert: insert if not found
            *(default False)*
        :return: update result
        """

    @abc.abstractmethod
    def find_and_modify(self, update=None, *, upsert=False,
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

    @abc.abstractmethod
    def remove(self, *, multi=True):
        """ Remove documents in queryset.

        :param bool multi: if False, remove only first finded document
            *(default True)*
        """

    @abc.abstractmethod
    def fields_all(self):
        """ Clear projection.
        """

    @abc.abstractmethod
    def sort(self, *sort):
        """ Return queryset with sorting.

        :param tuples sort: tuples with two items:
            `('field_name', sort_order_as_int)`.

        .. code:: python

            qs.sort(('field_1', 1), ('field_2', -1))
        """

    @abc.abstractmethod
    def distinct(self, field):
        """ Distinct query.

        :param str field: field for distinct
        :return: list with result data
        """

    @abc.abstractmethod
    def count(self):
        """ Count documents in queryset.

        :return: **int**
        """

    @abc.abstractmethod
    def join(self, *field_names):
        """ Create `yadm.Join` object, join `field_names` and return it.

        :param str fiels_names: fields for join
        :return: new :class:`yadm.join.Join`

        Next algorithm for join:
            1. Get all documents from queryset;
            2. Aggegate all ids from requested fields;
            3. Make *$in* queries for get joined documents;
            4. Bind joined documents to objects from first queryset;

        `Join` object is instance of `abc.Sequence`.
        """

    @abc.abstractmethod
    def ids(self):
        """ Return documents ids from queryset.
        """
