import structures

from yadm.fields.base import DatabaseFieldMixin
from yadm.serialize import to_mongo


class EmbeddedDocumentField(DatabaseFieldMixin, structures.Field):
    def __init__(self, embedded_document_class):
        self.embedded_document_class = embedded_document_class

    def func(self, value):
        if isinstance(value, dict):
            return self.embedded_document_class(**value)
        elif isinstance(value, self.embedded_document_class):
            return value
        else:
            raise TypeError('Only {!r} or dict is alowed, but {!r} given'
                ''.format(self.embedded_document_class, type(value)))

    def to_mongo(self, document, value):
        return to_mongo(value)

    def from_mongo(self, document, value):
        if not isinstance(value, self.embedded_document_class):
            return self.embedded_document_class(**value)
        else:
            return value
