"""
List of objects.

.. code-block:: python

    class Doc(Document):
        __collection__ = 'docs'
        integers = fields.ListField(fields.IntegerField)

    doc = Doc()
    doc.integers.append(1)
    doc.integers.append(2)
    assert doc.integers == [1, 2]

    db.insert(doc)
    doc = db.get_queryset(Doc).find_one(doc.id)  # reload

    doc.integers.append(3)  # do not save
    assert doc.integers == [1, 2, 3]
    doc = db.get_queryset(Doc).find_one(doc.id)  # reload
    assert doc.integers == [1, 2]

    doc.integers.remove(2)  # do not save too
    assert doc.integers == [1]
    doc = db.get_queryset(Doc).find_one(doc.id)  # reload
    assert doc.integers == [1, 2]

    doc.integers.push(3)  # $push query
    assert doc.integers == [1, 2, 3]
    doc = db.get_queryset(Doc).find_one(doc.id)  # reload
    assert doc.integers == [1, 2, 3]

    doc.integers.pull(2)  # $pull query
    assert doc.integers == [1, 3]
    doc = db.get_queryset(Doc).find_one(doc.id)  # reload
    assert doc.integers == [1, 3]

"""
from collections import abc

from yadm.fields.base import pass_null
from yadm.fields.containers import (
    Container,
    ContainerField,
)


class List(Container, abc.MutableSequence):
    """ Container for list.
    """
    def insert(self, index, item):
        """ Append item to list.

        :param int index:
        :param item: item for insert

        This method does not save object!
        """
        self._data.insert(index, self._prepare_item(index, item))
        self._set_changed()

    def append(self, item):
        """ Append item to list.

        :param item: item for append

        This method does not save object!
        """
        index = len(self)
        self._data.append(self._prepare_item(index, item))
        self._set_changed()

    def remove(self, item):
        """ Remove item from list.

        :param item: item for remove

        This method does not save object!
        """
        self._data.remove(item)
        self._set_changed()

    def push(self, item, reload=True):
        """ Push item directly to database.

        :param item: item for `$push`
        :param bool reload: automatically reload all values from database

        See `$push` in MongoDB's `update`.
        """
        index = len(self)
        item = self._prepare_item(index, item)
        data = self._item_field.to_mongo(self, item)

        qs = self._get_queryset()
        qs.update({'$push': {self.__field_name__: data}}, multi=False)
        self._data.append(item)

        if reload:
            self.reload()

    def pull(self, query, reload=True):
        """ Pull item from database.

        :param query: query for `$pull` on this field
        :param bool reload: automatically reload all values from database

        See `$pull` in MongoDB's `update`.
        """
        qs = self._get_queryset()
        qs.update({'$pull': {self.__field_name__: query}}, multi=False)

        if reload:
            self.reload()

    def replace(self, query, item, reload=True):
        """ Replace list elements.

        :param query: query for `update`.
            Keys of this query is relative.
        :param item: embedded document or dict
        :param bool reload: automatically reload all values from database
        """
        data = self._item_field.to_mongo(self, item)

        processed_query = {}
        for key, value in query.items():
            processed_query['.'.join([self.__field_name__, key])] = value

        qs = self._get_queryset()
        qs = qs.find(processed_query)
        qs.update({'$set': {'.'.join([self.__field_name__, '$']): data}})

        if reload:
            self.reload()

    def update(self, query, values, reload=True):
        """ Update fields in embedded documents.

        :param query: query for `update`.
            Keys of this query is relative.
        :param values: dict of new values
        :param bool reload: automatically reload all values from database
        """
        processed_query = {}
        for key, value in query.items():
            processed_query['.'.join([self.__field_name__, key])] = value

        data = {}
        for key, value in values.items():
            data['.'.join([self.__field_name__, '$', key])] = value

        qs = self._get_queryset()
        qs = qs.find(processed_query)
        qs.update({'$set': data})

        if reload:
            self.reload()


class ListField(ContainerField):
    """ Field for list values.

    For example, document with list of integers:

    .. code-block:: python

        class TestDoc(Document):
            __collection__ = 'testdoc'
            li = fields.ListField(fields.IntegerField())
    """
    container = List

    def get_default_value(self):
        return []

    def prepare_value(self, document, value):
        pi = self.prepare_item
        container = self.container(self, document, [])
        g = (pi(container, n, i) for n, i in enumerate(value))
        container._data.extend(g)
        return container

    @pass_null
    def to_mongo(self, document, value):
        tm = self.item_field.to_mongo
        return [tm(value, i) for i in value]

    @pass_null
    def from_mongo(self, document, value):
        fm = self.item_field.from_mongo
        sp = self._set_parent

        container = self.container(self, document, [])
        g = (sp(container, n, fm(container, i)) for n, i in enumerate(value))
        container._data.extend(g)
        return container
