from collections.abc import Mapping

from yadm.fields.base import Field


class UnmutableMap(Mapping):
    def __init__(self, data):
        self._data = data

    def __getitem__(self, item):
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):  # pragma: no cover
        return repr("{}({!r})".format(self.__class__.__name__, self._data))


class MongoMapField(Field):
    def __init__(self, smart_null=False):
        super().__init__(smart_null=smart_null)

    def get_fake(self, document, faker, deep):  # pragma: no cover
        return UnmutableMap({})

    def prepare_value(self, document, value):
        return UnmutableMap(value)

    def from_mongo(self, document, value):
        return UnmutableMap(value)

    def to_mongo(self, document, value):
        return dict(value)
