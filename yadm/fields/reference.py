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

from bson import ObjectId
import structures

from yadm.serialize import from_mongo
from yadm.fields import DatabaseFieldMixin


class ReferenceField(DatabaseFieldMixin, structures.Field):
    """ Field for work with references

    :param document_class: class for refered documents
    """
    def __init__(self, document_class):
        self.document_class = document_class

    def from_mongo(self, document, value):
        from yadm.documents import Document  # recursive imports

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
        from yadm.documents import Document  # recursive imports

        if isinstance(value, ObjectId):
            return value

        elif isinstance(value, dict):
            return value['_id']

        elif isinstance(value, Document):
            return value.id

        else:
            raise TypeError('value must be ObjectId, Document or dict')
