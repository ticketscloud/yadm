from yadm.fields.containers import (
    ContainerDescriptor,
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
                self._data.add(self._field.item_field.from_mongo(item))
            else:
                self._data.add(self._func(item))

    def add(self, item):
        """ Append item to list

        This method does not save object!
        """
        self._data.add(self._func(item))
        self._set_changed()

    def remove(self, item):
        """ Remove item from list

        This method does not save object!
        """
        self._data.remove(item)
        self._set_changed()

    def add_to_set(self, item):
        """ Add item directly to database

        See `$addToSet` in MongoDB's `update`.
        """
        item = self._func(item)
        qs = self._get_queryset()
        qs.update({'$addToSet': {self._field_name: item}}, multi=False)
        self._data.add(item)

    def pull(self, query, reload=True):
        """ Pull item from database

        See `$pull` in MongoDB's `update`.

        :reload: automatically reload all values from database
        """
        qs = self._get_queryset()
        qs.update({'$pull': {self._field_name: query}}, multi=False)

        if reload:
            doc = qs.find_one()
            self._load_from_mongo(getattr(doc, self._field_name))


class SetField(ArrayField):
    """ Field for list values

    For example, document with list of integers:

        class TestDoc(Document):
            __collection__ = 'testdoc'
            li = fields.ListField(fields.IntegerField())
    """
    container = Set

    @property
    def default(self):
        return set()
