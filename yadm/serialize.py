"""
Functions for serialize and deserialize data.
"""

from collections import defaultdict
from typing import Any, Union, Optional, Container, Iterable, Dict

from yadm.documents import MetaDocument, BaseDocument
from yadm.document_item import DocumentItemMixin
from yadm.exceptions import NotLoadedError
from yadm.markers import AttributeNotSet


LOOKUPS_KEY = '__yadm_lookups__'


TRaw = Dict[str, Any]


def to_mongo(document: BaseDocument,
             exclude: Optional[Container[str]] = None,
             include: Optional[Container[str]] = None,
             skip_not_loaded: bool = False) -> TRaw:
    """ Serialize document to MongoDB data.

    1. Lookup in exclude;
    2. Lookup in include;
    3. Lookup in __cache__;
    4. Lookup in __raw__;
    5. Lookup in __not_loaded__;
    6. Process values with '.' from include;
    """
    result = {}

    not_loaded = set()
    if document.__not_loaded__:
        for fn in document.__not_loaded__:
            not_loaded.add(fn.split('.')[0])  # first level only

    for name, field in document.__fields__.items():
        if exclude and name in exclude:
            continue

        elif include and name not in include:
            continue

        elif name in document.__cache__:
            value = document.__cache__[name]
            if value is not AttributeNotSet:
                raw = field.to_mongo(document, value)
                if raw is not AttributeNotSet:
                    result[name] = raw

        elif name in not_loaded:
            if skip_not_loaded:
                continue
            else:
                raise NotLoadedError(field, document)

        elif name in document.__raw__:
            result[name] = document.__raw__[name]

        else:
            continue  # pragma: no cover

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


def from_mongo(document_class: MetaDocument, raw: TRaw,
               not_loaded: Optional[Iterable[str]] = None,
               parent: Union[BaseDocument, DocumentItemMixin, None] = None,
               name: Optional[str] = None) -> BaseDocument:
    """ Deserialize MongoDB raw data to document.
    """
    document = document_class(__new_document__=False)
    document.__raw__ = raw
    document.__not_loaded__ = frozenset(not_loaded or frozenset())

    for field_name, field in document_class.__fields__.items():
        if (field_name not in raw and
                field_name not in document.__not_loaded__ and field.smart_null):
            document.__cache__[field_name] = None

    if LOOKUPS_KEY in raw:
        document.__yadm_lookups__ = raw.pop(LOOKUPS_KEY)

    if parent is not None:
        document.__parent__ = parent
        document.__name__ = name

    return document
