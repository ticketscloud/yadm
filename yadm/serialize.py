"""
Functions for serialize and deserialize data.
"""

from yadm.markers import NotLoaded


def to_mongo(document, exclude=(), include=None):
    """ Serialize document to MongoDB data

    :param BaseDocument document: document for serializing
    :param list exclude: exclude fields
    :param list include: include only fields (all by default)
    """
    result = {}

    for name, field in document.__fields__.items():
        if name in exclude:
            continue

        if include is not None and name not in include:
            continue

        if not hasattr(document, name):
            continue

        value = getattr(document, name)
        result[name] = field.to_mongo(document, value)

    if include:
        include_groups = {}

        for name in (f for f in include if '.' in f):
            first, last = name.split('.', 1)
            include_groups.setdefault(first, set()).add(last)

        for name, subinclude in include_groups.items():
            edoc = getattr(document, name)
            result[name] = to_mongo(edoc, include=subinclude)

    return result


def from_mongo(document_class, data, clear_fields_changed=True):
    """ Deserialize MongoDB data to document

    :param document_class: document class
    :param dict data: data from MongoDB
    :param bool clear_fields_changed: clear changed flags
        for new document (default True)
    """
    document = document_class(__initialized__=False)

    for name, field in document.__fields__.items():
        if name in data:
            value = data[name]
            document.__data__[name] = value

        else:
            document.__data__[name] = NotLoaded

    if clear_fields_changed:
        document.__fields_changed__.clear()

    document.__initialized__ = True
    return document
