"""
List of objects

.. code-block:: python

    class Doc(Document):
        __collection__ = 'docs'
        integers = fields.ListField(fields.IntegerField)

    doc = Doc()
    doc.integers.append(1)
    doc.integers.append(2)
    assert doc.integers == [1, 2]

    db.insert(doc)
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload

    doc.integers.append(3)  # do not save
    assert doc.integers == [1, 2, 3]
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.integers == [1, 2]

    doc.integers.remove(2)  # do not save too
    assert doc.integers == [1]
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.integers == [1, 2]

    doc.integers.push(3)  # $push query
    assert doc.integers == [1, 2, 3]
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.integers == [1, 2, 3]

    doc.integers.pull(2)  # $pull query
    assert doc.integers == [1, 3]
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.integers == [1, 3]

"""
from collections import abc

from yadm.fields.containers import (
    ArrayContainer,
    ArrayField,
)


class List(ArrayContainer, abc.MutableSequence):
    """ Container for list
    """
    def __repr__(self):
        return 'List({!r})'.format(self._data)

    def _load_from_mongo(self, data):
        self._data = []

        for item in data or ():
            if hasattr(self._field.item_field, 'from_mongo'):
                value = self._field.item_field.from_mongo(self.__parent__, item)
            else:
                value = self._prepare_value(item)

            self._data.append(value)

    def insert(self, index, item):
        """ Append item to list

        :param item: item for append

        This method does not save object!
        """
        self._data.insert(index, self._prepare_value(item))
        self._set_changed()

    def append(self, item):
        """ Append item to list

        :param item: item for append

        This method does not save object!
        """
        self._data.append(self._prepare_value(item))
        self._set_changed()

    def remove(self, item):
        """ Remove item from list

        :param item: item for remove

        This method does not save object!
        """
        self._data.remove(item)
        self._set_changed()

    def push(self, item):
        """ Push item directly to database

        :param item: item for `$push`

        See `$push` in MongoDB's `update`.
        """
        item = self._prepare_value(item)

        if hasattr(self._field.item_field, 'to_mongo'):
            data = self._field.item_field.to_mongo(self.__document__, item)
        else:
            data = item

        qs = self._get_queryset()
        qs.update({'$push': {self.__field_name__: data}}, multi=False)
        self._data.append(item)

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

    def replace(self, query, item, reload=True):
        """ Replace list elements

        :param query: query for `update`.
            Keys of this query is relative.
        :param item: embedded document or dict
        """
        if hasattr(self._field.item_field, 'to_mongo'):
            data = self._field.item_field.to_mongo(self.__document__, item)
        else:
            data = item

        processed_query = {}
        for key, value in query.items():
            processed_query['.'.join([self.__field_name__, key])] = value

        qs = self._get_queryset()
        qs = qs.find(processed_query)
        qs.update({'$set': {'.'.join([self.__field_name__, '$']): data}})

        if reload:
            doc = self._get_queryset().find_one()
            self._load_from_mongo(self.__get_value__(doc))

    def update(self, query, values, reload=True):
        """ Update fields in embedded documents

        :param query: query for `update`.
            Keys of this query is relative.
        :param values: dict of new values
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
            doc = self._get_queryset().find_one()
            self._load_from_mongo(self.__get_value__(doc))


class ListField(ArrayField):
    """ Field for list values

    For example, document with list of integers:

    .. code-block:: python

        class TestDoc(Document):
            __collection__ = 'testdoc'
            li = fields.ListField(fields.IntegerField())
    """
    container = List

    @property
    def default(self):
        return []
