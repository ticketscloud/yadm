"""
Basic documents classes for build models.

.. code-block:: python

    class User(Document):
        __collection__ = 'users'

        first_name = fields.StringField
        last_name = fields.StringField
        age = fields.IntegerField


All fields placed in :py:mod:`yadm.fields` package.
"""

from yadm.fields.base import Field
from yadm.fields.simple import ObjectIdField


class MetaDocument(type):
    '''Metaclass for documents'''
    def __init__(cls, name, bases, cls_dict):
        cls.__fields__ = {}

        for base in bases:
            if isinstance(base, MetaDocument):
                for name, field in base.__fields__.items():
                    if name not in cls_dict:
                        cls_dict[name] = field.copy()

        for attr, field in cls_dict.items():
            if isinstance(field, type) and issubclass(field, Field):
                field = field()

            if hasattr(field, 'contribute_to_class'):
                field.contribute_to_class(cls, attr)

            else:
                setattr(cls, attr, field)

        super().__init__(name, bases, cls_dict)


class BaseDocument(metaclass=MetaDocument):
    """ Base class for all documents
    """
    def __init__(self, **kwargs):
        self.__data__ = {}
        self.__fields_changed__ = set()

        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        """ Implement it for pretty str and repr documents
        """
        return str(id(self))

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, str(self))


class Document(BaseDocument):
    """ Class for build first level documents
    """
    __collection__ = None
    __db__ = None

    _id = ObjectIdField

    def __str__(self):
        if hasattr(self, '_id'):
            return '{!s}:{!s}'.format(self.__collection__, self._id)
        else:
            return '{!s}:<empty>'.format(self.__collection__)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @id.deleter
    def id(self, id):
        del self._id


class EmbeddedDocument(BaseDocument):
    """ Class for build embedded documents
    """
