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
"""
from yadm.common import EnclosedDocDescriptor
from yadm.markers import AttributeNotSet
from yadm.documents import Document
from yadm.fields.base import Field
from yadm.testing import create_fake


class BrokenReference(Exception):
    pass


class NotBindingToDatabase(Exception):
    pass


class ReferenceField(Field):
    """ Field for work with references

    :param reference_document_class: class for refered documents
    :param bool null: if True, None by default
    """
    reference_document_class = EnclosedDocDescriptor('reference')

    def __init__(self, reference_document_class, null=False):
        self.reference_document_class = reference_document_class
        self.null = null

    def get_default(self, document):
        if self.null:
            return None
        else:
            return AttributeNotSet

    def get_fake(self, document, faker, depth):
        res = create_fake(
            self.reference_document_class,
            __db__=document.__db__,
            __faker__=faker,
            __depth__=depth)

        if res is AttributeNotSet and self.null:
            return None
        else:
            return res

    def copy(self):
        return self.__class__(self.reference_document_class)

    def prepare_value(self, document, value):
        if isinstance(value, Document):
            return value
        elif value is AttributeNotSet:
            return AttributeNotSet
        else:
            return self.from_mongo(document, value)

    def from_mongo(self, document, value):
        if value is None:
            return None

        elif document.__db__ is not None:
            qs = document.__db__.get_queryset(self.reference_document_class)
            doc = qs.with_id(value)

            if doc is not None:
                return doc
            else:
                raise BrokenReference(value)

        else:
            raise NotBindingToDatabase((document, self, value))

    def to_mongo(self, document, value):
        if value is None:
            return None
        else:
            return value.id
