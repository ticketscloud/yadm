from collections import abc

from yadm.documents import Document
from yadm.fields.reference import ReferenceField


class Join(abc.Sequence):
    def __init__(self, qs):
        self._qs = qs
        self._document_class = qs._document_class
        self._db = qs._db

        self._data = list(qs)

    # abc.Sequence method

    def __iter__(self):
        return (i for i in self._data)

    def __getitem__(self, idx):
        return self._data[idx]

    def __contains__(self, item):
        return item in self._data

    def __len__(self):
        return len(self._data)

    def __reversed__(self):
        return reversed(self._data)

    def index(self, item):
        return self._data.index(item)

    # end abc.Sequence

    def join(self, field_name):
        document_fields = self._document_class.__fields__
        if field_name not in document_fields:
            raise ValueError(field_name)

        field = document_fields[field_name]

        if not isinstance(field, ReferenceField):
            raise RuntimeError('bad field type')

        joined_document_class = field.reference_document_class

        ids = self.get_ids(field_name)
        qs = self._db(joined_document_class).find({'_id': {'$in': ids}})
        joined_objects = qs.bulk()
        self.set_objects(field_name, joined_objects)

        return self

    def get_ids(self, field_name):
        ids = set()

        for doc in self:
            value = doc.__data__.get(field_name)

            if value is None:
                continue
            elif isinstance(value, Document):
                ids.add(value.id)
            else:
                ids.add(value)

        return list(ids)

    def set_objects(self, field_name, objects):
        for doc in self:
            value = doc.__data__.get(field_name)

            if value is None:
                continue
            elif isinstance(value, Document):
                _id = value.id
            else:
                _id = value

            doc.__data__[field_name] = objects.get(_id, value)
