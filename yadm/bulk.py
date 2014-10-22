from yadm.serialize import to_mongo


class Bulk:
    """ Bulk object

    Context manager:

        with db.bulk(Doc) as bulk:
            bulk.insert(doc_1)
            bulk.insert(doc_2)
    """
    def __init__(self, db, document_class, ordered=False):
        self._db = db
        self._document_class = document_class
        self._ordered = ordered
        self._collection = db._get_collection(self._document_class)

        if ordered:
            self._bulk = self._collection.initialize_ordered_bulk_op()
        else:
            self._bulk = self._collection.initialize_unordered_bulk_op()

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
        return self._bulk.execute()

    def insert(self, document):
        """ Add insert document to bulk

        :param Document document: document for insert

        .. warning::
            This unlike `Database.insert`!
            It not set `document.id` and `document.__db__`.
        """
        if not isinstance(document, self._document_class):
            raise TypeError("Bulk.insert() argument must be a {}, not '{}'"
                            "".format(self._document_class, document))

        self._bulk.insert(to_mongo(document))
