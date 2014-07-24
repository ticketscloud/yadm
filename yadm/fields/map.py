"""
Map

.. code-block:: python

    class Doc(Document):
        __collection__ = 'docs'
        map = fields.MapField(fields.IntegerField)

    doc = Doc()
    doc.map['a'] = 1
    doc.map['b'] = 2
    assert doc.map == {'a': 1, 'b': 2}

    db.insert(doc)
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload

    doc.map['c'] = 3  # do not save
    assert doc.map == {'a': 1, 'b': 2, 'c': 3}
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.map == {'a': 1, 'b': 2}

    del doc.map['b']  # do not save too
    assert doc.map == {'a': 1}
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.map == {'a': 1, 'b': 2}

    doc.map.set('d', 3)  # $set query
    assert doc.map == {'a': 1, 'b': 2, 'c': 3}
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.map == {'a': 1, 'b': 2, 'c': 3}

    doc.map.unset('d', 3)  # $unset query
    assert doc.map == {'a': 1, 'b': 2}
    doc = db.get_queryset(Doc).with_id(doc.id)  # reload
    assert doc.map == {'a': 1, 'b': 2}

"""
from collections import abc

from yadm.fields.containers import (
    Container,
    ContainerField,
)


class Map(Container, abc.MutableMapping):
    """ Map
    """

    def __repr__(self):
        return 'Map({!r})'.format(self._data)

    def _load_from_mongo(self, data):
        self._data = {}

        for key, value in (data or {}).items():
            if hasattr(self._field.value_field, 'from_mongo'):
                value = self._field.value_field.from_mongo(self.__parent__, value)
            else:
                value = self._prepare_value(value)

            self._data[key] = value

    def _prepare_value(self, value):
        """ `prepare_value` function for `value_field`
        """
        if hasattr(self._field.value_field, 'prepare_value'):
            return self._field.value_field.prepare_value(None, value)
        else:
            return value

    def set(self, key, value):
        """ Set key directly in database

        :param key: key
        :param value: value for `$set`

        See `$set` in MongoDB's `set`.
        """
        key = str(key)

        if hasattr(self._field.value_field, 'to_mongo'):
            value = self._field.value_field.to_mongo(self.__document__, value)
        else:
            value = self._prepare_value(value)

        qs = self._get_queryset()
        fn = '.'.join([self.__field_name__, key])
        qs.update({'$set': {fn: value}}, multi=False)
        self._data[key] = value

    def unset(self, key):
        """ Unset key directly in database

        :param key: key

        See `$unset` in MongoDB's `unset`.
        """
        key = str(key)
        qs = self._get_queryset()
        fn = '.'.join([self.__field_name__, key])
        qs.update({'$unset': {fn: True}}, multi=False)
        del self._data[key]


class MapField(ContainerField):
    """ Field for maps
    """
    container = Map

    def __init__(self, value_field):
        self.value_field = value_field

    @property
    def default(self):
        return {}

    def from_mongo(self, document, value):
        return self.container(document, self, value)

    def to_mongo(self, document, value):
        return value._data
