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
from yadm.markers import AttributeNotSet
from yadm.fields.base import Field
from yadm.serialize import to_mongo, from_mongo
from yadm.testing import create_fake


class EmbeddedDocumentField(Field):
    """ Field for embedded objects

    :param EmbeddedDocument embedded_document_class:
        class for embedded document
    :param bool auto_create: automatic creation embedded
        document from access
    """
    auto_create = True
    embedded_document_class = EnclosedDocDescriptor('embedded')

    def __init__(self, embedded_document_class, auto_create=True):
        self.embedded_document_class = embedded_document_class
        self.auto_create = auto_create

    def get_default(self, document):
        if self.auto_create:
            return self.embedded_document_class(
                __parent__=document,
                __name__=self.name,
            )
        else:
            return AttributeNotSet

    def get_fake(self, document, faker, depth):
        return create_fake(
            self.embedded_document_class,
            __parent__=document,
            __name__=self.name,
            __faker__=faker,
            __depth__=depth)

    def prepare_value(self, document, value):
        if value in [None, AttributeNotSet]:
            return value

        elif isinstance(value, dict):
            value = self.embedded_document_class(
                __parent__=document,
                __name__=self.name,
                **value)

        elif isinstance(value, self.embedded_document_class):
            value.__parent__ = document
            value.__name__ = self.name

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

        value = from_mongo(self.embedded_document_class, value,
                           parent=document, name=self.name)

        return value

    def copy(self):
        """ Return copy of field
        """
        return self.__class__(self.embedded_document_class)
