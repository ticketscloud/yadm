"""
Work with embedded documents.

.. code-block:: python

    class EDoc(EmbeddedDocument):
        i = fields.IntegerField()

    class Doc(Document):
        __collection__ = 'docs'
        edoc = EmbeddedDocumentField(EDoc)

    doc = Doc()
    doc.edoc = EDoc()
    doc.edoc.i = 13
    db.insert(doc)
"""

from yadm.fields.base import Field
from yadm.serialize import to_mongo


class EmbeddedDocumentField(Field):
    """ Field for embedded objects

    :param EmbeddedDocument embedded_document_class: class for embedded document
    """
    def __init__(self, embedded_document_class):
        self.embedded_document_class = embedded_document_class

    def prepare_value(self, value):
        if isinstance(value, dict):
            value = self.embedded_document_class(**value)
        elif not isinstance(value, self.embedded_document_class):
            raise TypeError('Only {!r} or dict is alowed, but {!r} given'
                            ''.format(self.embedded_document_class, type(value)))

        value.__name__ = self.name
        return value

    def to_mongo(self, document, value):
        return to_mongo(value)

    def from_mongo(self, document, value):
        if not isinstance(value, self.embedded_document_class):
            value = self.embedded_document_class(**value)

        value.__name__ = self.name
        return value
