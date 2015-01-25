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

from bson import ObjectId

from yadm.fields.containers import (
    Container,
    ContainerField,
)


class Map(Container, abc.MutableMapping):
    """ Map
    """
    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self._data)

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
        if isinstance(value, self.container):
            return value
        else:
            return self.container(document, self, value)

    def to_mongo(self, document, value):
        tm = self.value_field.to_mongo
        return {k: tm(document, v) for k, v in value._data.items()}


class MapCustomKeys(Map):
    def from_str(self, value):
        """ Cast value """
        raise NotImplementedError

    def _load_from_mongo(self, data):
        data = {str(k): v for k, v in data.items()}
        super()._load_from_mongo(data)

    def __iter__(self):
        from_str = self.from_str
        return (from_str(k) for k in super().__iter__())

    def __getitem__(self, key):
        return super().__getitem__(str(key))

    def __setitem__(self, key, value):
        super().__setitem__(str(key), value)

    def __delitem__(self, key):
        super().__delitem__(str(key))

    def __contains__(self, key):
        return str(key) in self._data


class MapCustomKeysField(MapField):
    container = MapCustomKeys

    def prepare_value(self, document, value):
        if value is None:
            return None
        elif isinstance(value, self.container):
            return value
        else:
            value = {str(k): v for k, v in value.items()}
            return self.container(document, self, value)


class MapIntKeys(MapCustomKeys):
    from_str = int


class MapIntKeysField(MapCustomKeysField):
    container = MapIntKeys


class MapObjectIdKeys(MapCustomKeys):
    from_str = ObjectId


class MapObjectIdKeysField(MapCustomKeysField):
    container = MapIntKeys
