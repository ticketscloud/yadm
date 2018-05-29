"""
Functions for serialize and deserialize data.
"""

from collections import defaultdict

from yadm.exceptions import NotLoadedError
from yadm.markers import AttributeNotSet


def to_mongo(document,
             exclude=None, include=None,
             skip_not_loaded=False,
             skip_default=False):
    """ Serialize document to MongoDB data.

    1. Lookup in exclude;
    2. Lookup in include;
    3. Lookup in __changed__;
    4. Lookup in __cache__;
    5. Lookup in __raw__;
    6. Lookup in __not_loaded__;
    7. Process values with '.' from include;
    """
    result = {}

    for name, field in document.__fields__.items():
        if exclude and name in exclude:
            continue

        elif include and name not in include:
            continue

        elif name in document.__changed__:
            value = document.__changed__[name]
            if value is not AttributeNotSet:
                raw = field.to_mongo(document, value)
                if raw is not AttributeNotSet:
                    result[name] = raw

        elif name in document.__cache__:
            value = document.__cache__[name]
            if value is not AttributeNotSet:
                raw = field.to_mongo(document, value)
                if raw is not AttributeNotSet:
                    result[name] = raw

        elif name in document.__raw__:
            result[name] = document.__raw__[name]

        elif (not skip_not_loaded) and (name in document.__not_loaded__):
            raise NotLoadedError(field, document)

        else:
            continue

    if include:
        # we need to go deeper (usable with $set)
        include_groups = defaultdict(set)

        for name in (f for f in include if '.' in f):
            first, last = name.split('.', 1)
            include_groups[first].add(last)

        for name, subinclude in include_groups.items():
            edoc = getattr(document, name)
            result[name] = to_mongo(edoc, include=subinclude)

    return result


def from_mongo(document_class, raw,
               not_loaded=None,
               parent=None, name=None):
    """ Deserialize MongoDB raw data to document.
    """
    document = document_class(__new_document__=False)
    document.__raw__ = raw
    document.__not_loaded__ = frozenset(not_loaded or frozenset())

    for field_name, field in document_class.__fields__.items():
        if field_name not in raw:
            if field_name not in document.__not_loaded__ and field.smart_null:
                document.__cache__[field_name] = None

    if parent is not None:
        document.__parent__ = parent
        document.__name__ = name

    return document
