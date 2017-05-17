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
import random

from yadm.common import EnclosedDocDescriptor
from yadm.documents import EmbeddedDocument
from yadm.markers import AttributeNotSet
from yadm.fields.base import Field, pass_null
from yadm.serialize import to_mongo, from_mongo
from yadm.testing import create_fake


class BaseEmbeddedDocumentField(Field):
    def get_embedded_document_class(self, document, value):
        """ Return class of embedded document for field.
        """
        raise NotImplementedError()

    @pass_null
    def prepare_value(self, document, value):
        if value is AttributeNotSet:
            return value

        elif isinstance(value, EmbeddedDocument):
            value.__parent__ = document
            value.__name__ = self.name

        else:
            raise TypeError("Only EmbeddedDocument is allowed, but {!r} given"
                            "".format(type(value)))

        return value

    @pass_null
    def to_mongo(self, document, value):
        return to_mongo(value)

    @pass_null
    def from_mongo(self, document, value):
        ed_class = self.get_embedded_document_class(document, value)
        return from_mongo(ed_class, value, parent=document, name=self.name)


class EmbeddedDocumentField(BaseEmbeddedDocumentField):
    """ Field for embedded objects.

    :param EmbeddedDocument embedded_document_class:
        class for embedded document
    :param bool auto_create: automatic creation embedded
        document from access
    """
    auto_create = True
    embedded_document_class = EnclosedDocDescriptor('embedded')

    def __init__(self, embedded_document_class, *,
                 auto_create=True, **kwargs):
        super().__init__(**kwargs)
        self.embedded_document_class = embedded_document_class
        self.auto_create = auto_create

    def get_embedded_document_class(self, document=None, value=None):
        return self.embedded_document_class

    def get_if_attribute_not_set(self, document):
        """ Call if key not exist in document.

        If auto_create is True, create and return new
        embedded document. Else AttributeError is raised.
        """
        if self.auto_create:
            ed_class = self.get_embedded_document_class(document)
            return ed_class(__parent__=document, __name__=self.name)
        else:
            raise AttributeError(self.name)

    def get_fake(self, document, faker, depth):
        return create_fake(
            self.get_embedded_document_class(document),
            __parent__=document,
            __name__=self.name,
            __faker__=faker,
            __depth__=depth,
        )

    @pass_null
    def prepare_value(self, document, value):
        if value is AttributeNotSet:
            return value

        ed_class = self.get_embedded_document_class(document, value)

        if isinstance(value, dict):
            value = ed_class(__parent__=document, __name__=self.name, **value)

        elif isinstance(value, ed_class):
            value.__parent__ = document
            value.__name__ = self.name

        else:
            raise TypeError("Only {!r}, dict or None is allowed, but {!r} given"
                            "".format(ed_class, type(value)))

        return value

    def copy(self):
        """ Return copy of field.
        """
        ed_class = self.get_embedded_document_class()
        return self.__class__(ed_class, smart_null=self.smart_null)


class TypedEmbeddedDocumentField(BaseEmbeddedDocumentField):
    """ Field for embedded document with variable types.

    :param str type_field: name of field in embedded document
        for select type
    :param dict types: map of type names to embedded document classes
    """
    type_field = None
    types = None

    def __init__(self, type_field=None, types=None, **kwargs):
        super().__init__(**kwargs)

        self.type_field = type_field or self.type_field
        self.types = types or self.types

        if self.types is None:
            raise TypeError("type attribute is not set")
        elif self.type_field is None:
            raise TypeError("type_field attribute is not set")

    def get_embedded_document_class(self, document, value):
        type_name = value.get(self.type_field, AttributeNotSet)
        ed_class = self.types.get(type_name, None)

        if ed_class is None:
            raise ValueError("Not found document type for {!r}"
                             "".format(type_name))
        else:
            return ed_class

    def get_fake(self, document, faker, depth):
        type_name = random.choice(list(self.types))
        ed_class = self.get_embedded_document_class(
            document=document,
            value={self.type_field: type_name},
        )

        return create_fake(
            ed_class,
            __parent__=document,
            __name__=self.name,
            __faker__=faker,
            __depth__=depth,
            **{self.type_field: type_name}
        )


class SimpleEmbeddedDocumentField(EmbeddedDocumentField):
    """ Field for simply create embedded documents.

    Usage:

        class Doc(Document):
            embedded = SimpleEmbeddedDocumentField({
                'i': IntegerField(),
                's': StringField(),
            })
    """
    embedded_document_class = None

    def __init__(self, fields, *, auto_create=True, **kwargs):
        if not isinstance(fields, dict):
            raise TypeError("First argument must be a dict, not {}"
                            "".format(type(fields)))
        elif not fields:
            raise ValueError("fields is empty")

        self.fields = fields

        super().__init__(None, auto_create=auto_create, **kwargs)

    def contribute_to_class(self, document_class, name):
        super().contribute_to_class(document_class, name)

        self.embedded_document_class = type(
            '{}__{}'.format(document_class.__name__, name),
            (EmbeddedDocument,),
            self.fields,
        )
