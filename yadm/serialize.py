"""
Functions for serialize and deserialize data.
"""

from collections import defaultdict

from yadm.markers import AttributeNotSet, NotLoaded


def _to_mongo_value(result, document, field, name, value):
    if value is not AttributeNotSet:
        result[name] = field.to_mongo(document, value)


def to_mongo(document, exclude=(), include=None):
    """ Serialize document to MongoDB data.

    :param BaseDocument document: document for serializing
    :param list exclude: exclude fields
    :param list include: include only fields (all by default)
    """
    result = {}

    for name, field in document.__fields__.items():
        if name in exclude:
            continue

        elif include is not None and name not in include:
            continue

        elif name in document.__changed__:
            value = document.__changed__[name]
            _to_mongo_value(result, document, field, name, value)

        elif name in document.__raw__:
            value = document.__raw__[name]
            if value is AttributeNotSet:
                continue

            result[name] = document.__raw__[name]

        elif name in document.__cache__:
            value = document.__cache__[name]
            _to_mongo_value(result, document, field, name, value)

        else:
            value = field.get_default(document)
            document.__changed__[name] = value  # lazy load default value
            _to_mongo_value(result, document, field, name, value)

    if include:
        include_groups = defaultdict(set)

        for name in (f for f in include if '.' in f):
            first, last = name.split('.', 1)
            include_groups[first].add(last)

        for name, subinclude in include_groups.items():
            edoc = getattr(document, name)
            result[name] = to_mongo(edoc, include=subinclude)

    return result


def from_mongo(document_class, data, not_loaded=(), parent=None, name=None):
    """ Deserialize MongoDB data to document.

    :param document_class: document class
    :param dict data: data from MongoDB
    :param list not_loaded: fields,
        who marked as not loaded
    :param parent: parent for new document
    :param str name: name for new document
    """
    document = document_class()

    for field_name, field in document_class.__fields__.items():
        if field_name in data:
            document.__raw__[field_name] = data[field_name]

        elif field_name in not_loaded:
            document.__raw__[field_name] = NotLoaded

        elif field.smart_null:
            document.__raw__[field_name] = None

        else:
            document.__raw__[field_name] = AttributeNotSet

    if parent is not None:
        document.__parent__ = parent
        document.__name__ = name

    return document
