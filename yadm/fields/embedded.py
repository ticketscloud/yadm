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

from yadm.common import EnclosedDocDescriptor
from yadm.fields.base import Field
from yadm.serialize import to_mongo, from_mongo


class EmbeddedDocumentField(Field):
    """ Field for embedded objects

    :param EmbeddedDocument embedded_document_class: class for embedded document
    """

    embedded_document_class = EnclosedDocDescriptor('embedded')

    def __init__(self, embedded_document_class):
        self.embedded_document_class = embedded_document_class

    def prepare_value(self, document, value):
        if value is None:
            return None

        elif isinstance(value, dict):
            value = self.embedded_document_class(
                __parent__=document,
                __name__=self.name,
                **value)

        elif not isinstance(value, self.embedded_document_class):
            raise TypeError('Only {!r}, dict or None is alowed, but {!r} given'
                            ''.format(self.embedded_document_class, type(value)))

        return value

    def to_mongo(self, document, value):
        if value is None:
            return None
        else:
            return to_mongo(value)

    def from_mongo(self, document, value):
        if value is None:
            return None

        elif not isinstance(value, self.embedded_document_class):
            value = from_mongo(self.embedded_document_class, value, clear_fields_changed=True)

        value.__parent__ = document
        value.__name__ = self.name
        return value

    def copy(self):
        """ Return copy of field
        """
        return self.__class__(self.embedded_document_class)
