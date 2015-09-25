from collections import abc
from collections import defaultdict

from bson import ObjectId

from yadm.documents import Document
from yadm.fields.reference import ReferenceField


class Join(abc.Sequence):
    """ Helper for build joins
    """
    def __init__(self, qs):
        self._qs = qs
        self._document_class = qs._document_class
        self._db = qs._db

        self._indexes = defaultdict(dict)
        self._map_name_type = {}
        self._map_type_names = defaultdict(set)
        self._map_name_ids = defaultdict(set)

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

    def get_queryset(self, field_name):
        """ Return queryset for joined objects
        """
        field = self._get_field(field_name)
        qs = self._db(field.reference_document_class)
        ids = self._get_joined_ids(field_name)
        return qs.find({'_id': {'$in': list(ids)}})

    def join(self, *field_names):
        """ Do manual join
        """
        self._load_names_types_maps(*field_names)
        self._load_map_name_ids(*field_names)
        self._load_objects_to_indexes(*field_names)
        self._set_objects(*field_names)

    def _get_field(self, field_name):
        document_fields = self._document_class.__fields__
        if field_name not in document_fields:
            raise ValueError(field_name)

        field = document_fields[field_name]

        if not isinstance(field, ReferenceField):
            raise RuntimeError('bad field type: {!r}'.format(field_name))

        return field

    def _get_joined_ids(self, field_name):
        ids = set()
        for doc in self:
            value = doc.__raw__[field_name]

            _id = self._prepare_id(value)
            if _id:
                ids.add(_id)

        return ids

    def _load_names_types_maps(self, *field_names):
        for field_name in field_names:
            field = self._get_field(field_name)
            reference_document_class = field.reference_document_class
            self._map_name_type[field_name] = reference_document_class
            self._map_type_names[reference_document_class].add(field_name)

    def _load_map_name_ids(self, *field_names):
        for doc in self:
            for field_name in field_names:
                value = doc.__raw__.get(field_name)

                _id = self._prepare_id(value)
                if _id:
                    self._map_name_ids[field_name].add(_id)

    def _load_objects_to_indexes(self, *field_names):
        field_names = set(field_names)

        for joined_document_class, names in self._map_type_names.items():
            if not (field_names & names):
                continue

            ids = set()
            for field_name in names:
                ids.update(self._map_name_ids[field_name])

            ids = list(ids - set(self._indexes[joined_document_class]))
            qs = self._db(joined_document_class).find({'_id': {'$in': ids}})
            self._indexes[joined_document_class].update(qs.bulk())

    def _set_objects(self, *field_names):
        for doc in self:
            for field_name in field_names:
                value = doc.__raw__.get(field_name)

                _id = self._prepare_id(value)
                if _id:
                    joined_document_class = self._map_name_type[field_name]
                    index = self._indexes[joined_document_class]
                    doc.__cache__[field_name] = index.get(_id, value)

    def _prepare_id(self, value):
        if isinstance(value, ObjectId):
            return value
        elif isinstance(value, Document):
            return value.id
