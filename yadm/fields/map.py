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

from yadm.markers import AttributeNotSet
from yadm.fields.containers import (
    Container,
    ContainerField,
)


class Map(Container, abc.MutableMapping):
    """ Map
    """
    def set(self, key, value, reload=True):
        """ Set key directly in database

        :param key: key
        :param value: value for `$set`

        See `$set` in MongoDB's `set`.
        """
        value = self._prepare_item(key, value)
        qs = self._get_queryset()
        fn = '.'.join([self.__field_name__, key])
        qs.update({'$set': {fn: value}}, multi=False)
        self._data[key] = value

        if reload:
            self.reload()

    def unset(self, key, reload=True):
        """ Unset key directly in database

        :param key: key

        See `$unset` in MongoDB's `unset`.
        """
        qs = self._get_queryset()
        fn = '.'.join([self.__field_name__, key])
        qs.update({'$unset': {fn: True}}, multi=False)
        del self._data[key]

        if reload:
            self.reload()


class MapField(ContainerField):
    """ Field for maps
    """
    container = Map

    def get_default_value(self):
        return {}

    def prepare_value(self, document, value):
        if value is AttributeNotSet:
            return AttributeNotSet

        pi = self.prepare_item
        container = self.container(self, document, {})

        if isinstance(value, dict):
            items = value.items()
        else:
            items = value

        container._data.update((k, pi(container, k, i)) for k, i in items)
        return container

    def to_mongo(self, document, value):
        tm = self.item_field.to_mongo
        return {k: tm(value, i) for k, i in value.items()}

    def from_mongo(self, document, value):
        fm = self.item_field.from_mongo
        sp = self._set_parent

        container = self.container(self, document, {})
        g = ((k, sp(container, k, fm(container, i))) for k, i in value.items())
        container._data.update(g)
        return container


class MapCustomKeys(Map):
    def __init__(self, field, parent, value):
        k2s = field.key_to_str
        pi = field.prepare_item
        value = {k2s(k): pi(self, k, v) for k, v in value.items()}

        super().__init__(field, parent, value)
        self.key_factory = field.key_factory
        self.key_to_str = field.key_to_str

    def __iter__(self):
        key_factory = self.key_factory
        return (key_factory(k) for k in super().__iter__())

    def __getitem__(self, item):
        return super().__getitem__(self.key_to_str(item))

    def __setitem__(self, item, value):
        super().__setitem__(self.key_to_str(item), value)

    def __delitem__(self, item):
        super().__delitem__(self.key_to_str(item))

    def __contains__(self, item):
        return self.key_to_str(item) in self._data

    def __eq__(self, other):
        if other is None or other is AttributeNotSet:
            return False
        elif isinstance(other, dict):
            other = self.__class__(self._field, self.__parent__, other)

        return self._data == other._data

    def set(self, key, value, reload=True):
        return super().set(self.key_to_str(key), value, reload)

    def unset(self, key, value, reload=True):
        return super().unset(self.key_to_str(key), value, reload)


class MapCustomKeysField(MapField):
    """ Field for maps with custom key type

    :param field item_field:
    :param func key_factory: function, who return thue key
        from raw string key
    :param func key_to_str:
    :param bool auto_create:
    """
    container = MapCustomKeys

    def __init__(self,
                 item_field,
                 key_factory,
                 key_to_str=str,
                 auto_create=True):
        super().__init__(item_field, auto_create=auto_create)
        self.key_factory = key_factory
        self.key_to_str = key_to_str

    def prepare_value(self, document, value):
        if value is AttributeNotSet:
            return AttributeNotSet

        pi = self.prepare_item
        k2s = self.key_to_str

        container = self.container(self, document, {})

        if isinstance(value, dict):
            items = value.items()
        else:
            items = value

        container._data.update((k2s(k), pi(container, k, i)) for k, i in items)
        return container

    def to_mongo(self, document, value):
        tm = self.item_field.to_mongo
        k2s = self.key_to_str
        return {k2s(k): tm(value, i) for k, i in value.items()}
