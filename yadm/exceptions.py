class NotLoadedError(Exception):
    """ Raise if value marked as not loaded.

    .. code:: python

        doc = db(Doc).fields('a').find_one()
        try:
            doc.b
        except NotLoadedError as exc:
            print("Raised!")
            assert exc.field is Doc.b
            assert exc.document is doc
    """
    def __init__(self, field, document):
        super().__init__(field, document)
        self.field = field
        self.document = document
