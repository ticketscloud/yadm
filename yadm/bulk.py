from collections.abc import Sequence

from pymongo.errors import BulkWriteError

from yadm.common import BaseResult, build_update_query
from yadm.serialize import to_mongo, from_mongo


class Bulk:
    """ Bulk object.

    :param Database db: Database instance
    :param MetaDocument document_class: document class for collection
    :param bool ordered: create ordered bulk (default `False`)
    :param bool raise_on_errors: raise BulkWriteError exception
        if write errors (default `True`)

    Context manager example:

        with db.bulk(Doc, ordered=True) as bulk:
            bulk.insert(doc_1)
            bulk.insert(doc_2)
            bulk.update_one(doc_3, inc={'incr_key': 1})
            bulk.find({'key': 'value'}).update(set={'key': 'new_value'})
            bulk.find({'key': 'new_value'}).remove()
    """
    _result = None
    _error = False

    def __init__(self, db, document_class,
                 ordered, raise_on_errors,
                 collection_params):
        self._db = db
        self._document_class = document_class
        self._ordered = ordered
        self._raise_on_errors = raise_on_errors

        self._collection = db._get_collection(self._document_class,
                                              params=collection_params)

        if ordered:
            self._bulk_mongo = self._collection.initialize_ordered_bulk_op()
        else:
            self._bulk_mongo = self._collection.initialize_unordered_bulk_op()

    def __repr__(self):
        return "<{}.{}('{}') at {}>".format(
            self.__class__.__module__,
            self.__class__.__name__,
            self._collection.name,
            hex(id(self)),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.execute()

    @property
    def result(self):
        """ A BulkResult instance or rise RuntimeError if not executed.
        """
        if self._result is not None:
            return self._result
        else:
            raise RuntimeError("The bulk was not executed.")

    @property
    def error(self):
        """ True for executed errors.
        """
        return self._error

    def execute(self):
        """ Execute the bulk query.

        :return: :py:class:`BulkResult` instance
        """
        try:
            raw_data = self._bulk_mongo.execute()
        except BulkWriteError as exc:
            self._error = True
            raw_data = exc.details

        self._result = BulkResult(self, raw_data)

        if self._error and self._raise_on_errors:
            raise BulkWriteError(raw_data)

        return self._result

    def insert(self, document):
        """ Add insert document to bulk.

        :param Document document: document for insert

        .. warning::
            This unlike :py:class:`Database.insert <yadm.database.Database.insert>`!
            Currently, it is not bind objects to database and set id.
        """
        if not isinstance(document, self._document_class):
            raise TypeError("Bulk.insert() argument must be a {}, not '{}'"
                            "".format(self._document_class, document.__class__))

        self._bulk_mongo.insert(to_mongo(document))

    def find(self, query):
        """ Start "find" query in bulk.

        :param dict query:
        :return: BulkQuery instance
        """
        return BulkQuery(self._bulk_mongo, query)

    def update_one(self, document, *,
                   set=None, unset=None, inc=None,
                   push=None, pull=None):
        """ Add update one document to bulk.
        """
        if not isinstance(document, self._document_class):
            raise TypeError("Bulk.update_one() first argument must be a {}, not '{}'"
                            "".format(self._document_class, document.__class__))

        self.find({'_id': document.id}).update(set=set, unset=unset, inc=inc,
                                               push=push, pull=pull)


class BulkQuery:
    def __init__(self, bulk_mongo, query, *, upsert=False):
        self._bulk_mongo = bulk_mongo
        self._query = query
        self._upsert = upsert

    def __repr__(self):
        if not self._upsert:
            return "{s.__class__.__name__}({s._query!r})".format(s=self)
        else:
            return "{s.__class__.__name__}({s._query!r} upsertable)".format(s=self)

    def upsert(self):
        return self.__class__(self._bulk_mongo, self._query, upsert=True)

    def update(self, *,
               set=None, unset=None, inc=None,
               push=None, pull=None):
        """ Updale documents finded in query.
        """
        update_query = build_update_query(set=set, unset=unset, inc=inc,
                                          push=push, pull=pull)

        if self._upsert:
            self._bulk_mongo.find(self._query).upsert().update(update_query)
        else:
            self._bulk_mongo.find(self._query).update(update_query)

    def remove(self):
        self._bulk_mongo.find(self._query).remove()


class BulkResult(BaseResult):
    """ Object who provide result of `Bulk.execute()`.
    """
    def __init__(self, bulk, raw):
        super().__init__(raw)
        self._bulk = bulk

    def __bool__(self):
        return not self._bulk.error

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self._raw)

    @property
    def n_inserted(self):
        """ Provide `nInserted` from raw result.
        """
        return self['nInserted']

    @property
    def n_upserted(self):
        """ Provide `nUpserted` from raw result.
        """
        return self['nUpserted']

    @property
    def n_modified(self):
        """ Provide `nModified` from raw result.
        """
        return self['nModified']

    @property
    def n_removed(self):
        """ Provide `nRemoved` from raw result.
        """
        return self['nRemoved']

    @property
    def write_errors(self):
        """ Provide `writeErrors` from raw result.
        """
        return _BulkResultWriteErrors(self._bulk, self._raw['writeErrors'])

    @property
    def write_concern_errors(self):
        # TODO
        return self._raw['writeConcernErrors']

    @property
    def upserted(self):
        # TODO
        return self._raw['upserted']


class _BulkResultWriteErrors(Sequence):
    def __init__(self, bulk, raw_data):
        self._bulk = bulk
        self._raw = raw_data

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            raise TypeError("Slicing not supported")
        else:
            return _BulkResultWriteErrorsItem(self._bulk, self._raw[idx])

    def __len__(self):
        return len(self._raw)


class _BulkResultWriteErrorsItem(BaseResult):
    _document_cache = None

    def __init__(self, bulk, raw):
        super().__init__(raw)
        self._bulk = bulk

    @property
    def index(self):
        return self['index']

    @property
    def code(self):
        return self['code']

    @property
    def errmsg(self):
        return self['errmsg']

    @property
    def op(self):
        return self['op']

    @property
    def document(self):
        if self._document_cache is None:
            document_class = self._bulk._document_class
            self._document_cache = from_mongo(document_class, self.op)

        return self._document_cache
