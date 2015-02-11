"""
Work with references.

.. code-block:: python

    class RDoc(Document):
        i = fields.IntegerField

    class Doc(Document):
        rdoc = fields.ReferenceField(RDoc)

    rdoc = RDoc()
    rdoc.i = 13
    db.insert(rdoc)

    doc = Doc()
    doc.rdoc = rdoc
    db.insert(doc)

    doc = db.get_queryset(Doc).with_id(doc.id)  # reload doc
    assert doc.rdoc.id == rdoc.id
    assert doc.rdoc.i == 13


TODO: many2many collections
"""
from yadm.common import EnclosedDocDescriptor
from yadm.documents import Document
from yadm.markers import NoDefault
from yadm.fields.base import Field, FieldDescriptor
from yadm.serialize import from_mongo


class ReferenceFieldDescriptor(FieldDescriptor):
    """ Descriptor for ReferenceField

    Save document in `document.__fields__` after first get.
    """
    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)

        if instance is not None and isinstance(value, Document):
            instance.__data__[self.name] = value

        return value


class ReferenceField(Field):
    """ Field for work with references

    :param reference_document_class: class for refered documents
    """

    descriptor_class = ReferenceFieldDescriptor
    reference_document_class = EnclosedDocDescriptor('reference')

    def __init__(self, reference_document_class, default=NoDefault):
        self.reference_document_class = reference_document_class
        self.default = default

    def copy(self):
        return self.__class__(self.reference_document_class)

    def from_mongo(self, document, value):
        if value is None:
            return None

        elif isinstance(value, dict):
            return from_mongo(self.reference_document_class, value)

        elif isinstance(value, Document):
            return value

        else:
            if document.__db__ is not None:
                qs = document.__db__.get_queryset(self.reference_document_class)
                doc = qs.with_id(value)

                if doc:
                    return doc
                else:
                    field = self.reference_document_class.__fields__['_id']
                    return field.prepare_value(None, value)

            else:
                return value

    def to_mongo(self, document, value):
        if isinstance(value, dict):
            return value['_id']

        elif isinstance(value, Document):
            return value.id

        else:
            return value
