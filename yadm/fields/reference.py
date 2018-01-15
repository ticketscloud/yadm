"""
Work with references.

.. code-block:: python

    class RDoc(Document):
        i = fields.IntegerField()

    class Doc(Document):
        rdoc = fields.ReferenceField(RDoc)

    rdoc = RDoc()
    rdoc.i = 13
    db.insert(rdoc)

    doc = Doc()
    doc.rdoc = rdoc
    db.insert(doc)

    doc = db.get_queryset(Doc).find_one(doc.id)  # reload doc
    assert doc.rdoc.id == rdoc.id
    assert doc.rdoc.i == 13

Or with asyncio:

.. code-block:: python

    rdoc = await doc.rdoc
    assert rdoc.id == rdoc.id
    assert rdoc.i == 13
    assert doc.rdoc == rdoc.id

"""
from bson import ObjectId
from bson.errors import InvalidId

from yadm.common import EnclosedDocDescriptor
from yadm.markers import AttributeNotSet
from yadm.documents import Document, DocumentItemMixin
from yadm.fields.base import Field, pass_null
from yadm.testing import create_fake


class BrokenReference(Exception):
    """ Raise if referrenced document is not found.
    """


class NotBindingToDatabase(Exception):  # noqa
    """ Raise if set ObjectId insted referenced document
    to new document, who not binded to database.
    """


class ReferenceField(Field):
    """ Field for work with references.

    :param reference_document_class: class for refered documents
    """
    reference_document_class = EnclosedDocDescriptor('reference')

    def __init__(self, reference_document_class, **kwargs):
        super().__init__(**kwargs)
        self.reference_document_class = reference_document_class

    def get_default(self, document):
        if self.smart_null:
            return None
        else:
            return AttributeNotSet

    def get_fake(self, document, faker, depth):
        """ Try create referenced document.
        """
        res = create_fake(
            self.reference_document_class,
            __db__=document.__db__,
            __faker__=faker,
            __depth__=depth)

        if res is AttributeNotSet and self.smart_null:
            return None
        else:
            return res

    def copy(self):
        return self.__class__(self.reference_document_class,
                              smart_null=self.smart_null)

    @pass_null
    def prepare_value(self, document, value):
        if isinstance(value, Document):
            return value
        elif value is AttributeNotSet:
            return AttributeNotSet
        else:
            if isinstance(value, str):
                try:
                    value = ObjectId(value)
                except InvalidId:
                    pass

            return self.from_mongo(document, value)

    @pass_null
    def from_mongo(self, document, value):
        if document.__db__ is not None:
            rdc = self.reference_document_class

            if document.__qs__ is not None:
                cache = document.__qs__.cache
            else:
                cache = {}  # fake cache

            if (rdc, value) in cache:
                return cache[(rdc, value)]
            else:
                if document.__db__.aio:
                    cache[(rdc, value)] = ref = Reference(value, document, self)
                    return ref
                else:
                    qs = document.__db__.get_queryset(rdc, cache=cache)
                    doc = qs.find_one(value)
                    if doc is None:  # pragma: no cover
                        doc = qs.read_primary().find_one(value, exc=BrokenReference)

                    cache[(rdc, value)] = doc
                    return doc

        else:
            raise NotBindingToDatabase((document, self, value))

    @pass_null
    def to_mongo(self, document, value):
        return value.id


class Reference(ObjectId):
    """ Reference object.

    This is awaitable:

        doc = await doc.reference
    """
    document = None

    def __init__(self,
                 _id: ObjectId,
                 parent: DocumentItemMixin,
                 field: ReferenceField):
        super().__init__(_id)
        self.parent = parent
        self.field = field
        self.db = parent.__db__
        self.document_class = field.reference_document_class

    def __repr__(self):
        n = self.__class__.__name__
        collection = self.document_class.__collection__
        status = '+' if self.document is not None else '-'
        return "{}({}:{} {})".format(n, collection, str(self), status)

    def __await__(self):
        return self.get().__await__()

    async def get(self, force: bool=False):
        if self.document is None or force:
            self.document = await self.db(self.document_class).find_one(self)
            if self.document is None:  # pragma: no cover
                self.document = await self.db.get_document(self.document_class, self)

        return self.document
