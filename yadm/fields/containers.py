"""
Base classes for containers.
"""

from yadm.fields import FieldDescriptor, Field
from yadm.documents import DocumentItemMixin


class ContainerDescriptor(FieldDescriptor):
    """ Descriptor for containers
    """
    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)

        if (instance is not None
                and not isinstance(instance.__data__[self.name], self.field.__class__)):

            instance.__data__[self.name] = value

        return value


class Container(DocumentItemMixin):
    """ Base class for containers
    """
    def __init__(self, parent, field, data):
        self.__parent__ = parent
        self._field = field
        self._field_name = field.name
        self._load_from_mongo(data)

    def __iter__(self):
        for n, item in enumerate(self._data):
            if isinstance(item, DocumentItemMixin):
                item.__parent__ = self
                item.__name__ = n

            yield item

    def __getitem__(self, item):
        value = self._data[item]

        if isinstance(value, DocumentItemMixin):
            value.__parent__ = self
            value.__name__ = item

        return value

    def __setitem__(self, item, value):
        self._data[item] = self._prepare_value(value)
        self._set_changed()

    def __delitem__(self, item):
        del self._data[item]
        self._set_changed()

    def __contains__(self, item):
        return item in self._data

    def __len__(self):
        return len(self._data)

    def __bool__(self):
        return bool(self._data)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._data == other._data
        else:
            return self._data == other

    def _prepare_value(self, item):
        """ `prepare_value` function for `item_field`
        """
        raise NotImplementedError

    def _load_from_mongo(self, data):
        """ Load data from pymongo

        Simply, here create self._data with deserialized data from mongo.
        """
        self._data = data

    def _get_queryset(self):
        """ Return queryset for got data for this field
        """
        if self.__db__ is None:
            raise RuntimeError('object not binded to database')

        qs = self.__db__.get_queryset(self.__document__.__class__)
        qs = qs.find({'_id': self.__document__.id})
        return qs.fields(self.__field_name__)

    def _set_changed(self):
        """ Add field to __fields_changed__

        TODO: work with deep objects
        """
        self.__parent__.__fields_changed__.add(self._field_name)


class ArrayContainer(Container):
    def __getitem__(self, item):
        if isinstance(item, slice):
            res = []
            start = item.start
            step = item.step

            for n, value in enumerate(self._data[item]):
                if isinstance(value, DocumentItemMixin):
                    value.__parent__ = self
                    value.__name__ = start + (step * n)

                res.append(value)

            return res

        else:
            return super().__getitem__(item)

    def _prepare_value(self, item):
        """ `prepare_value` function for `item_field`
        """
        return self._field.item_field.prepare_value(None, item)


class ContainerField(Field):
    """ Base class for container fields
    """
    descriptor_class = ContainerDescriptor

    @property
    def default(self):
        """ Return default value

        Must be implemented in field class.
        """
        raise NotImplementedError

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
        raise NotImplementedError


class ArrayField(ContainerField):
    """ Base class for array containers like as lists or set
    """
    container = ArrayContainer

    def __init__(self, item_field):
        if isinstance(item_field, type):
            item_field = item_field()

        self.item_field = item_field

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
