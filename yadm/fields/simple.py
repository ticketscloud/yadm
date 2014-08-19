"""
Fields for basic data types.
"""

from bson import ObjectId

from yadm.fields.base import Field, DefaultMixin
from yadm.markers import NoDefault


class SimpleField(DefaultMixin, Field):
    """ Base field for simple types

    :param default: default value
    :param set choices: set of possible values
    """
    type = NotImplemented
    choices = None

    def __init__(self, default=NoDefault, choices=None):
        if self.type is NotImplemented:
            raise ValueError('Attribute "type" not implemented!')

        self.choices = choices

        super().__init__(default)

    def prepare_value(self, document, value):
        if not isinstance(value, self.type) and value is not None:
            value = self.type(value)

        if self.choices is not None and value not in self.choices:
            raise ValueError('value not in choices: {!r}'.format(value))

        return value

    def to_mongo(self, document, value):
        return self.prepare_value(document, value)

    def from_mongo(self, document, value):
        return self.prepare_value(document, value)


class ObjectIdField(SimpleField):
    """ Field for ObjectId

    :param bool default_gen: generate default value if not set
    """
    type = ObjectId

    def __init__(self, default_gen=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_gen = default_gen

    @property
    def default(self):
        if self.default_gen:
            return ObjectId()
        else:
            return NoDefault


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
