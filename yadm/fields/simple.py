"""
Fields for basic data types.
"""

from bson import ObjectId

from yadm.fields.base import Field, DefaultMixin
from yadm.markers import AttributeNotSet


class SimpleField(DefaultMixin, Field):
    """ Base field for simple types

    :param default: default value
    :param set choices: set of possible values
    """
    type = NotImplemented
    choices = None

    def __init__(self, default=AttributeNotSet, choices=None):
        if self.type is NotImplemented:
            raise ValueError('Attribute "type" is not implemented!')

        self.choices = choices

        super().__init__(default=default)

        if default is not AttributeNotSet:
            self._check_choices(default)

    def prepare_value(self, document, value):
        if value is AttributeNotSet:
            return AttributeNotSet

        elif not isinstance(value, self.type) and value is not None:
            value = self.type(value)

        self._check_choices(value)
        return value

    def _check_choices(self, value):
        if self.choices is not None and value not in self.choices:
            raise ValueError("{!r} not in choices: {!r}"
                             "".format(value, self.choices))


class ObjectIdField(SimpleField):
    """ Field for ObjectId

    :param bool default_gen: generate default value if not set
    """
    type = ObjectId
    default_gen = False

    def __init__(self, default_gen=False):
        super().__init__()
        self.default_gen = default_gen

    def get_default(self, document):
        # import ipdb; ipdb.set_trace()
        if self.default_gen:
            return ObjectId()
        else:
            return AttributeNotSet

    def copy(self):
        return self.__class__(default_gen=self.default_gen)


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
