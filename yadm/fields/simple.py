"""
Fields for basic data types.
"""
from random import choice

from bson import ObjectId

from yadm.fields.base import Field, DefaultMixin, pass_null
from yadm.markers import AttributeNotSet


class StaticField(Field):
    """ Field for static data.
    """
    def __init__(self, data):
        self.data = data

    def get_default(self, document):
        return self.data

    def get_if_attribute_not_set(self, document):
        raise RuntimeError("value for {} not exist in database"
                           "".format(self.__class__.__name__))

    def copy(self):
        return self.__class__(self.data)

    def prepare_value(self, document, value):
        raise AttributeError("'{}' object has no attribute '{}'"
                             "".format(document.__class__.__name__,
                                       self.name))

    def to_mongo(self, document, value):
        if value != self.data:
            raise RuntimeError("bad value for {}: {!r} != {!r}"
                               "".format(self.__class__.__name__,
                                         value, self.data))
        else:
            return self.data

    def from_mongo(self, document, value):
        if value != self.data:
            raise RuntimeError("bad value in database for {}: {!r} != {!r}"
                               "".format(self.__class__.__name__,
                                         value, self.data))
        else:
            return self.data


class SimpleField(DefaultMixin, Field):
    """ Base field for simple types.

    :param default: default value
    :param set choices: set of possible values
    """
    type = NotImplemented
    choices = None

    def __init__(self, default=AttributeNotSet, *, choices=None, **kwargs):
        if self.type is NotImplemented:
            raise NotImplementedError("Attribute 'type' is not implemented!")

        self.choices = choices

        kwargs['default'] = default
        super().__init__(**kwargs)

        if default is not AttributeNotSet:
            self._check_choices(default)

    def get_fake(self, document, faker, depth):
        if self.choices is not None:
            return choice(self.choices)
        else:
            return super().get_fake(document, faker, depth)

    @pass_null
    def prepare_value(self, document, value):
        if value is AttributeNotSet:
            return AttributeNotSet

        elif not isinstance(value, self.type):
            value = self.type(value)

        self._check_choices(value)
        return value

    @pass_null
    def from_mongo(self, document, value):
        if value is AttributeNotSet:
            return AttributeNotSet

        elif not isinstance(value, self.type):
            value = self.type(value)

        return value

    def _check_choices(self, value):
        if self.choices is not None and value not in self.choices:
            raise ValueError("{!r} not in choices: {!r}"
                             "".format(value, self.choices))


class ObjectIdField(SimpleField):
    """ Field for ObjectId.

    :param bool default_gen: generate default value if not set
    """
    type = ObjectId
    default_gen = False

    def __init__(self, default_gen=False):
        super().__init__()
        self.default_gen = default_gen

    def get_default(self, document):
        if self.default_gen:
            return ObjectId()
        else:
            return AttributeNotSet

    def get_fake(self, document, faker, depth):
        return ObjectId()

    def copy(self):
        return self.__class__(default_gen=self.default_gen)


class BooleanField(SimpleField):
    """ Field for boolean values.
    """
    type = bool

    def get_fake(self, document, faker, depth):
        return faker.pybool()


class IntegerField(SimpleField):
    """ Field for integer.
    """
    type = int

    def get_fake(self, document, faker, depth):
        if self.choices is not None:
            return choice(self.choices)
        else:
            return faker.pyint()


class FloatField(SimpleField):
    """ Field for float.
    """
    type = float

    def get_fake(self, document, faker, depth):
        if self.choices is not None:
            return choice(self.choices)
        else:
            return faker.pyfloat()


class StringField(SimpleField):
    """ Field for string.
    """
    type = str

    def get_fake(self, document, faker, depth):
        if self.choices is not None:
            return choice(self.choices)

        try:
            fake = getattr(faker, self.name)()
        except AttributeError:
            fake = None

        if isinstance(fake, str):
            return fake
        else:
            return faker.pystr()
