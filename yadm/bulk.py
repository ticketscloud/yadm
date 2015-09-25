from collections.abc import Sequence

from pymongo.errors import BulkWriteError

from yadm.common import BaseResult
from yadm.serialize import to_mongo, from_mongo


class Bulk:
    """ Bulk object

    :param Database db: Database instance
    :param MetaDocument document_class: document class for collection
    :param bool ordered: create ordered bulk (default `False`)
    :param bool raise_on_errors: raise BulkWriteError exception
        if write errors (default `True`)

    Context manager:

        with db.bulk(Doc) as bulk:
            bulk.insert(doc_1)
            bulk.insert(doc_2)
    """
    result = None
    error = False

    def __init__(self, db, document_class,
                 ordered=False, raise_on_errors=True):
        self._db = db
        self._document_class = document_class
        self._ordered = ordered
        self._raise_on_errors = raise_on_errors

        self._collection = db._get_collection(self._document_class)

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

    def execute(self):
        try:
            raw_data = self._bulk_mongo.execute()
        except BulkWriteError as exc:
            self.error = True
            raw_data = exc.details

        self.result = BulkResult(self, raw_data)

        if self.error and self._raise_on_errors:
            raise BulkWriteError(raw_data)

        return self.result

    def insert(self, document):
        """ Add insert document to bulk

        :param Document document: document for insert

        .. warning::
            This unlike `Database.insert`!
            It not set `document.id` and `document.__db__`.
        """
        if not isinstance(document, self._document_class):
            raise TypeError("Bulk.insert() argument must be a {}, not '{}'"
                            "".format(self._document_class, document.__class__))

        self._bulk_mongo.insert(to_mongo(document))


class BulkResult(BaseResult):
    """ Object who provide result of `Bulk.execute()`
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
        return self['nInserted']

    @property
    def n_upserted(self):
        return self['nUpserted']

    @property
    def n_modified(self):
        return self['nModified']

    @property
    def n_removed(self):
        return self['nRemoved']

    @property
    def write_errors(self):
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
