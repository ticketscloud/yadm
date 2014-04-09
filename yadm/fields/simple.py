"""
Fields for basic data types.
"""

from bson import ObjectId
import structures

from yadm.fields.base import Field


class ObjectIdField(Field):
    """ Field for ObjectId

    :param bool default_gen: generate default value if not set
    """
    @staticmethod
    def func(value):
        if isinstance(value, ObjectId):
            return value
        else:
            return ObjectId(value)

    def __init__(self, default_gen=False):
        super().__init__()
        self.default_gen = default_gen

    @property
    def default(self):
        if self.default_gen:
            return ObjectId()
        else:
            return structures.markers.NoDefault


class SimpleField(Field):
    """ Base field for simple types
    """
    type = NotImplemented

    def __init__(self, default=structures.markers.NoDefault):
        if self.type is NotImplemented:
            raise ValueError('Attribute "type" not implemented!')

        super().__init__(default)

    @classmethod
    def func(cls, value):
        if isinstance(value, cls.type):
            return value
        else:
            return cls.type(value)


class BooleanField(SimpleField):
    """ Field for boolean values
    """
    type = bool


class IntegerField(SimpleField):
    """ Field for integer
    """
    type = int


class FloatField(SimpleField):
    """ Field for float
    """
    type = float


class StringField(SimpleField):
    """ Field for string
    """
    type = str
