from yadm.fields.containers import (
    ContainerDescriptor,
    Container,
    ArrayField,
)


class List(Container):
    """ Container for list
    """
    def __repr__(self):
        return 'List({!r})'.format(self._data)

    def _load_from_mongo(self, data):
        self._data = []

        for item in data:
            if hasattr(self._field.item_field, 'from_mongo'):
                self._data.append(self._field.item_field.from_mongo(item))
            else:
                self._data.append(self._func(item))

    def append(self, item):
        """ Append item to list

        This method does not save object!
        """
        self._data.append(self._func(item))
        self._set_changed()

    def remove(self, item):
        """ Remove item from list

        This method does not save object!
        """
        self._data.remove(item)
        self._set_changed()

    def push(self, item):
        """ Push item directly to database

        See `$push` in MongoDB's `update`.
        """
        item = self._func(item)
        qs = self._get_queryset()
        qs.update({'$push': {self._field_name: item}}, multi=False)
        self._data.append(item)

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


class ListField(ArrayField):
    """ Field for list values

    For example, document with list of integers:

        class TestDoc(Document):
            __collection__ = 'testdoc'
            li = fields.ListField(fields.IntegerField())
    """
    container = List

    @property
    def default(self):
        return []
