"""
Base classes for containers.
"""

import structures

from yadm.fields import DatabaseFieldDescriptor, Field


class ContainerDescriptor(DatabaseFieldDescriptor):
    """ Descriptor for containers
    """
    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)

        if (instance is not None
                and not isinstance(instance.__data__[self.name], self.field.__class__)):

            instance.__data__[self.name] = value

        return value


class Container:
    """ Base class for containers
    """
    def __init__(self, document, field, data):
        self._document = document
        self._field = field
        self._field_name = field.name
        self._load_from_mongo(data)

    def _func(self, item):
        """ `func` function for `item_field`
        """
        return self._field.item_field.func(item)

    def _load_from_mongo(self, data):
        """ Load data from pymongo

        Simply, here create self._data with deserialized data from mongo.
        """
        self._data = data
        return NotImplemented

    def _get_queryset(self):
        """ Return queryset for got data for this field
        """
        qs = self._document.__db__.get_queryset(self._document.__class__)
        qs = qs.find({'_id': self._document.id})
        return qs.fields(self._field_name)

    def _set_changed(self):
        """ Add field to __fields_changed__
        """
        self._document.__fields_changed__.add(self._field_name)  # set!

    def __iter__(self):
        return (i for i in self._data)

    def __getitem__(self, item):
        return self._data[item]

    def __len__(self):
        return len(self._data)

    def __bool__(self):
        return bool(self._data)

    def __eq__(self, other):
        return self._data == other


class ContainerField(Field):
    """ Base class for container fields
    """
    descriptor_class = ContainerDescriptor

    def __init__(self, item_field):
        self.item_field = item_field

    @property
    def default(self):
        """ Return default value

        Must be implemented in field class.
        """
        return NotImplemented

    def to_mongo(self, document, value):
        """ Serialize field value to data for MongoDB

        Must be implemented in field class.

        :param BaseDocument document: document with this value
        :param value: python value
        :return: MongoDB data
        """
        return value

    def from_mongo(self, document, value):
        """ Deserialize field value from MongoDB data

        Must be implemented in field class.

        :param BaseDocument document: document with this value
        :param value: MongoDB data
        :return: python value
        """
        return NotImplemented


class ArrayField(ContainerField):
    """ Base class for array containers like as lists or set
    """
    container = Container

    def to_mongo(self, document, value):
        """ See :py:meth:`ContainerField.to_mongo`
        """
        result = []

        for item in value:
            if hasattr(self.item_field, 'to_mongo'):
                result.append(self.item_field.to_mongo(document, item))
            else:
                result.append(item)

        return result

    def from_mongo(self, document, value):
        """ See :py:meth:`ContainerField.from_mongo`
        """
        return self.container(document, self, value)
