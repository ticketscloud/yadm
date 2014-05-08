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

from bson import ObjectId

from yadm.fields.base import Field
from yadm.fields.simple import ObjectIdField


class MetaDocument(type):

    '''Metaclass for documents'''
    def __init__(cls, name, bases, cls_dict):
        cls.__data__ = {}
        cls.__fields_changed__ = set()
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
    __initialized__ = False

    def __init__(self, **kwargs):

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.__initialized__ = kwargs.get('__initialized__', True)

    def __str__(self):
        """ Implement it for pretty str and repr documents
        """
        return str(id(self))

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, str(self))


class Document(BaseDocument):

    """ Class for build first level documents

    .. py:attribute:: __collection__

        Name of MongoDB collection

    .. py:attribute:: _id

        Mongo object id (:py:class:`bson.ObjectId`)

    .. py:attribute:: id

        Alias for :py:attr:`_id` for simply use

    .. py:attribute:: __db__

        Internal attribute contain instance of :py:class:`yadm.database.Database`
        for realize :py:class:`yadm.fields.references.ReferenceField`.
        It bind in :py:class:`yadm.database.Database` or :py:class:`yadm.queryset.QuerySet`.
    """
    __collection__ = None
    __db__ = None

    _id = ObjectIdField

    def __str__(self):
        if hasattr(self, '_id'):
            return '{!s}:{!s}'.format(self.__collection__, self._id)
        else:
            return '{!s}:<empty>'.format(self.__collection__)

    def __eq__(self, other):
        if isinstance(other, Document):
            return self.id == other.id
        elif isinstance(other, ObjectId):
            return self.id == other
        else:
            return False

    def __hash__(self):
        return hash(self.id)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @id.deleter
    def id(self, id):
        del self._id


class DocumentItemMixin:

    """ Mixin for custom all fields values,
    such as :py:class:`EmbeddedDocument`, :py:class:`yadm.fields.containers.Container`

    .. py:attribute:: __parent__

        Parent object.

        .. code-block:: python

            assert doc.embedded_doc.__parent__ is doc
            assert doc.list[13].__parent__ is doc.list

    .. py:attribute:: __name__

        .. code-block:: python

            assert doc.list.__name__ == 'list'
            assert doc.list[13].__name__ == 13

    """
    __parent__ = None
    __name__ = None

    @property
    def __document__(self):
        """ Root document

        .. code-block:: python

                assert doc.f.l[0].__document__ is doc
        """
        obj = self

        while getattr(obj, '__parent__', None):
            obj = obj.__parent__

        return obj

    @property
    def __db__(self):
        """ Database object

        .. code-block:: python

            assert doc.f.l[0].__db__ is doc.__db__
        """
        return self.__document__.__db__

    @property
    def __path__(self):
        """ Path to root generator

        .. code-block:: python

            assert list(doc.f.l[0].__path__) == [doc.f.l[0], doc.f.l, doc.f]
        """
        obj = self

        while getattr(obj, '__parent__', None):
            yield obj
            obj = obj.__parent__

    @property
    def __path_names__(self):
        """ Path to root generator

        .. code-block:: python

            assert list(doc.f.l[0].__path__) == [0, 'l', 'f']
        """
        for item in self.__path__:
            yield item.__name__

    @property
    def __field_name__(self):
        """ Dotted field name for MongoDB opperations,
        like as $set, $push and other...

        .. code-block:: python

            assert doc.f.l[0].__field_name__ == 'f.l.0'
        """
        return '.'.join(reversed([str(i) for i in self.__path_names__]))

    def __get_value__(self, document):
        """ Get value from document with path to self
        """
        obj = document

        for name in reversed(list(self.__path_names__)):
            if isinstance(name, int):
                obj = obj[name]
            else:
                obj = getattr(obj, name)

        return obj


class EmbeddedDocument(DocumentItemMixin, BaseDocument):

    """ Class for build embedded documents
    """
