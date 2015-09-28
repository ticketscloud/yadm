"""
Field with sets.

Similar as :py:mod:`yadm.fields.list`.
"""
from collections import abc

from yadm.fields.containers import Container
from yadm.fields.list import ListField


class Set(Container, abc.MutableSet):
    """ Container for set
    """
    def __getitem__(self, item):
        raise TypeError("'{}' object does not support indexing"
                        "".format(self.__class__.__name__))

    def __setitem__(self, item, value):
        raise TypeError("'{}' object does not support item assignment"
                        "".format(self.__class__.__name__))

    def __delitem__(self, item):
        raise TypeError("'{}' object doesn't support item deletion"
                        "".format(self.__class__.__name__))

    def __eq__(self, other):
        if isinstance(other, set):
            return set(self) == set(other)
        else:
            return False

    def add(self, item):
        """ Append item to set

        :param item: item for add

        This method does not save object!
        """
        item = self._prepare_item(len(self), item)
        if item not in self._data:
            self._data.append(item)

        self._set_changed()

    def discard(self, item):
        """ Remove item from the set if it is present

        :param item: item for discard

        This method does not save object!
        """
        try:
            self._data.remove(item)
        except ValueError:
            pass

        self._set_changed()

    def remove(self, item):
        """ Remove item from set

        :param item: item for remove

        This method does not save object!
        """
        try:
            self._data.remove(item)
        except ValueError as exc:
            raise KeyError from exc

        self._set_changed()

    def add_to_set(self, item, reload=True):
        """ Add item directly to database

        :param item: item for `$addToSet`
        :param bool reload: automatically reload all values from database

        See `$addToSet` in MongoDB's `update`.
        """

        index = len(self)
        item = self._prepare_item(index, item)
        data = self._field.item_field.to_mongo(self.__document__, item)

        qs = self._get_queryset()
        qs.update({'$addToSet': {self.__field_name__: data}}, multi=False)
        self.add(item)

        if reload:
            self.reload()

    def pull(self, query, reload=True):
        """ Pull item from database

        :param query: query for `$pull` on this field
        :param bool reload: automatically reload all values from database

        See `$pull` in MongoDB's `update`.
        """
        qs = self._get_queryset()
        qs.update({'$pull': {self.__field_name__: query}}, multi=False)

        if reload:
            self.reload()


class SetField(ListField):
    """ Field for set values
    """
    container = Set
