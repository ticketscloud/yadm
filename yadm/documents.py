"""
Basic documents classes for build models.

    class User(Document):
        __collection__ = 'users'

        first_name = fields.StringField()
        last_name = fields.StringField()
        age = fields.IntegerField()


All fields placed in :py:mod:`yadm.fields` package.
"""
from typing import Union, Optional, Any, Generator, Dict

from bson import ObjectId
from faker import Faker

from yadm.fields.base import Field
from yadm.fields.simple import ObjectIdField
from yadm.document_item import DocumentItemMixin
from yadm.log_items import BaseLog


class DocumentLog(BaseLog):
    pass


class MetaDocument(type):
    """ Metaclass for documents.
    """
    def __init__(cls, name: str, bases: tuple, cls_dict: dict):  # noqa
        cls.__fields__ = {}

        for base in bases:
            if isinstance(base, MetaDocument):
                for name, field in base.__fields__.items():
                    if name not in cls_dict:
                        cls_dict[name] = field.copy()

        for attr, field in cls_dict.items():
            if isinstance(field, type) and issubclass(field, Field):
                raise TypeError("Field must be instantiated: {}.{}".format(
                                cls.__name__, attr))

            if isinstance(field, Field):
                field.contribute_to_class(cls, attr)
            else:
                setattr(cls, attr, field)

        super().__init__(name, bases, cls_dict)


class BaseDocument(metaclass=MetaDocument):
    """ Base class for all documents.
    """
    __raw__: dict
    __cache__: dict
    __not_loaded__: frozenset = frozenset()

    def __init__(self,
                 *args,
                 __new_document__: bool = True,
                 **kwargs):
        if args:
            if len(args) != 1:
                raise TypeError("only one positional argument accepted!")

            elif not isinstance(args[0], dict):
                name = type(args[0]).__name__
                raise TypeError("argument must be a dict, not {}".format(name))

            data = args[0]

        elif kwargs:
            data = kwargs

        else:
            data = {}

        self.__raw__ = {}
        self.__cache__ = {}

        for key, field in self.__fields__.items():
            if key in data:
                setattr(self, key, data[key])
            elif __new_document__:  # default values for new objects
                self.__cache__[key] = field.get_default(self)

    def __str__(self) -> str:
        return repr(self)

    def __repr__(self) -> str:  # pragma: no cover
        return '{}({})'.format(self.__class__.__name__, str(hex(id(self))))

    def __fake__(
        self,
        values: dict,
        faker: Faker,
        depth: int,
    ) -> Optional[Generator[Optional[dict], None, None]]:
        """ Fake data customizer.
        """
        # # pre pocessor and prepare values
        # yield values  # send new values
        # # post processor
        # yield
        # # post save processor

    def __debug_print__(self):  # pragma: no cover
        """ Print debug information.
        """
        from pprint import pprint
        pprint({
            'repr': repr(self),
            'raw': self.__raw__,
            'not_loaded': self.__not_loaded__,
            'cache': self.__cache__,
            'log': self.__log__,
        })


class Document(BaseDocument):
    """ Class for build first level documents.
    """
    __collection__: str
    __default_projection__: Optional[Dict[str, Any]] = None
    __new_document__: bool = True
    __log__: DocumentLog
    __db__: 'yadm.database.BaseDatabase'
    __qs__: 'QuerySet' = None

    __yadm_lookups__: dict

    _id = ObjectIdField()

    def __init__(self,
                 *args,
                 __db__: Optional['yadm.database.BaseDatabase'] = None,
                 __new_document__: bool = True,
                 **kwargs):
        self.__db__ = __db__
        self.__new_document__ = __new_document__
        self.__yadm_lookups__ = {}
        self.__log__ = DocumentLog()
        super().__init__(*args, __new_document__=__new_document__, **kwargs)

    def __repr__(self) -> str:
        _id = getattr(self, '_id', '<new>')
        return '{}({})'.format(self.__class__.__name__, _id)

    def __eq__(self, other: Union['Document', ObjectId]) -> bool:
        if isinstance(other, Document):
            return self.id == other.id
        elif isinstance(other, ObjectId):
            return self.id == other
        else:
            return False

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def id(self) -> Optional[ObjectId]:
        return self._id

    @id.setter
    def id(self, id: ObjectId):
        self._id = id

    @id.deleter
    def id(self):  # pragma: no cover
        del self._id


class EmbeddedDocument(DocumentItemMixin, BaseDocument):
    """ Class for build embedded documents.
    """
    def __init__(self,
                 *args,
                 __parent__: Union[BaseDocument, DocumentItemMixin, None] = None,
                 __name__: Optional[str] = None,
                 **kwargs):
        self.__parent__ = __parent__
        self.__name__ = __name__
        super().__init__(*args, **kwargs)

    @property
    def __new_document__(self) -> bool:  # pragma: no cover
        return self.__document__.__new_document__
