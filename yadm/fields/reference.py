from bson import ObjectId
import structures

from yadm.serialize import from_mongo
from yadm.fields import DatabaseFieldMixin, DatabaseFieldDescriptor


class ReferenceFieldDescriptor(DatabaseFieldDescriptor):
    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)

        if instance is not None:
            return self.field.from_mongo(instance, value)
        else:
            return value

    def __set__(self, instance, value):
        super().__set__(instance, value)


class ReferenceField(DatabaseFieldMixin, structures.Field):
    def __init__(self, document_class):
        self.document_class = document_class

    def from_mongo(self, document, value):
        from yadm.documents import Document

        if isinstance(value, ObjectId):
            if document.__db__ is not None:
                return document.__db__.get_queryset(self.document_class).with_id(value)
            else:
                return value

        elif isinstance(value, dict):
            return from_mongo(self.document_class, value)

        elif isinstance(value, Document):
            return value

        else:
            raise TypeError('value must be ObjectId, Document or dict')


    def to_mongo(self, document, value):
        from yadm.documents import Document

        if isinstance(value, ObjectId):
            return value

        elif isinstance(value, dict):
            return value['_id']

        elif isinstance(value, Document):
            return value.id

        else:
            raise TypeError('value must be ObjectId, Document or dict')


    def contribute_to_structure(self, structure, name):
        super().contribute_to_structure(structure, name)
        setattr(structure, name, ReferenceFieldDescriptor(name, self))
