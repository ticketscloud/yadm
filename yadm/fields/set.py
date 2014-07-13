"""
Field with sets.

Similar as :py:mod:`yadm.fields.list`.
"""
from collections import abc

from yadm.fields.containers import (
    ArrayContainer,
    ArrayField,
)


class Set(ArrayContainer, abc.MutableSet):
    """ Container for set
    """
    def __repr__(self):
        return 'Set({!r})'.format(self._data)

    def _load_from_mongo(self, data):
        self._data = set()

        for item in data or ():
            if hasattr(self._field.item_field, 'from_mongo'):
                value = self._field.item_field.from_mongo(self.__document__, item)
            else:
                value = self._prepare_value(item)

            self._data.add(value)

    def add(self, item):
        """ Append item to set

        :param item: item for add

        This method does not save object!
        """
        self._data.add(self._prepare_value(item))
        self._set_changed()

    def discard(self, item):
        """ Remove item from the set if it is present

        :param item: item for discard

        This method does not save object!
        """
        self._data.discard(item)
        self._set_changed()

    def remove(self, item):
        """ Remove item from set

        :param item: item for remove

        This method does not save object!
        """
        self._data.remove(item)
        self._set_changed()

    def add_to_set(self, item):
        """ Add item directly to database

        :param item: item for `$addToSet`

        See `$addToSet` in MongoDB's `update`.
        """
        item = self._prepare_value(item)

        if hasattr(self._field.item_field, 'to_mongo'):
            data = self._field.item_field.to_mongo(self.__document__, item)
        else:
            data = item

        qs = self._get_queryset()
        qs.update({'$addToSet': {self.__field_name__: data}}, multi=False)
        self._data.add(item)

    def pull(self, query, reload=True):
        """ Pull item from database

        :param query: query for `$pull` on this field
        :param bool reload: automatically reload all values from database

        See `$pull` in MongoDB's `update`.
        """
        qs = self._get_queryset()
        qs.update({'$pull': {self.__field_name__: query}}, multi=False)

        if reload:
            doc = qs.find_one()
            self._load_from_mongo(self.__get_value__(doc))


class SetField(ArrayField):
    """ Field for set values
    """
    container = Set

    @property
    def default(self):
        return set()
