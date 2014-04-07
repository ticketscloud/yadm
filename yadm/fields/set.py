"""
Field with sets.

Similar as :py:mod:`yadm.fields.list`.
"""

from yadm.fields.containers import (
    Container,
    ArrayField,
)


class Set(Container):
    """ Container for set
    """
    def __repr__(self):
        return 'Set({!r})'.format(self._data)

    def _load_from_mongo(self, data):
        self._data = set()

        for item in data:
            if hasattr(self._field.item_field, 'from_mongo'):
                self._data.add(self._field.item_field.from_mongo(None, item))
            else:
                self._data.add(self._func(item))

    def add(self, item):
        """ Append item to set

        :param item: item for add

        This method does not save object!
        """
        self._data.add(self._func(item))
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
        item = self._func(item)
        qs = self._get_queryset()
        qs.update({'$addToSet': {self._field_name: item}}, multi=False)
        self._data.add(item)

    def pull(self, query, reload=True):
        """ Pull item from database

        :param query: query for `$pull` on this field
        :param bool reload: automatically reload all values from database

        See `$pull` in MongoDB's `update`.
        """
        qs = self._get_queryset()
        qs.update({'$pull': {self._field_name: query}}, multi=False)

        if reload:
            doc = qs.find_one()
            self._load_from_mongo(getattr(doc, self._field_name))


class SetField(ArrayField):
    """ Field for set values
    """
    container = Set

    @property
    def default(self):
        return set()
