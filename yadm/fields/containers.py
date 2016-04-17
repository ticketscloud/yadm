"""
Base classes for containers.
"""
from yadm.markers import AttributeNotSet
from yadm.fields.base import Field
from yadm.documents import DocumentItemMixin


class Container(DocumentItemMixin):
    """ Base class for containers.
    """
    def __init__(self, field, parent, value):
        self.__name__ = field.name
        self.__parent__ = parent
        self._field = field
        self._item_field = field.item_field
        self._data = value

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        return self._data[item]

    def __setitem__(self, item, value):
        self._data[item] = self._prepare_item(item, value)
        self._set_changed()

    def __delitem__(self, item):
        del self._data[item]
        self._set_changed()

    def __contains__(self, item):
        return item in self._data

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self._data)

    def __eq__(self, other):
        if isinstance(other, Container):
            return self._data == other._data
        else:
            return self._data == other

    def _set_changed(self):
        self._field.set_parent_changed(self)

    def _prepare_item(self, item, value):
        return self._field.prepare_item(self, item, value)

    def _get_queryset(self):
        """ Return queryset for got data for this field.
        """
        if self.__db__ is None:
            raise RuntimeError('object not binded to database')

        qs = self.__db__.get_queryset(self.__document__.__class__)
        qs = qs.find({'_id': self.__document__.id})
        return qs.fields(self.__field_name__)

    def reload(self):
        """ Reload all object from database.
        """
        if len(list(self.__path__)) > 1:
            raise ValueError("can't reload deep objects: {}"
                             "".format(self.__field_name__))

        doc = self._get_queryset().find_one()
        self._data = self.__get_value__(doc)._data


class ContainerField(Field):
    """ Base class for container fields.
    """
    container = Container

    def __init__(self, item_field=None, *, auto_create=True, **kwargs):
        super().__init__(**kwargs)

        if not isinstance(item_field, (Field, type(None))):
            raise TypeError("first argument must be field isinstance or None,"
                            " but {}".format(item_field))

        self.item_field = item_field
        self.auto_create = auto_create

    def get_default(self, document):
        if self.auto_create:
            return self.container(self, document, self.get_default_value())
        else:
            return AttributeNotSet

    def prepare_item(self, container, item, value):
        if self.item_field is not None:
            value = self.item_field.prepare_value(container, value)
            self._set_parent(container, item, value)
            return value
        else:
            raise NotImplementedError(
                "item_field is None, but prepare_item is not implemented")

    def prepare_value(self, document, value):
        # return self.container(self, document, value)
        raise NotImplementedError

    def get_default_value(self):
        raise NotImplementedError

    def to_mongo(self, document, value):
        # return value._data
        raise NotImplementedError

    def from_mongo(self, document, value):
        # return self.container(self, document, value)
        raise NotImplementedError

    def _set_parent(self, container, name, value):
        if isinstance(value, DocumentItemMixin):
            value.__parent__ = container
            value.__name__ = name

        return value
