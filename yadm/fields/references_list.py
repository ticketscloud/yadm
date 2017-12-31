""" Field for list with references.

Usage:

    class Doc(Document):
        refs = ReferencesListField(RefDoc)


    doc = db(Doc).find_one(...)
    doc.ref.resolve()  # resolve all documents in with query
    for ref_doc in doc.ref:
        ...

With asyncio:

    await doc.ref.resolve()

doc.ref is a ReferencesList instance, witch subclass MutableSequence.
But without resolving NotResolved raised for any actions with it
(except __len__ and __bool__).

"""
from collections.abc import MutableSequence

from yadm.documents import DocumentItemMixin
from yadm.queryset import NotFoundBehavior
from yadm.fields.base import Field


class NotResolved(Exception):
    pass


class AlreadyResolved(Exception):
    pass


class ReferencesList(MutableSequence, DocumentItemMixin):
    _resolved = False
    _changed = False

    def __init__(self, reference_document_class, ids: list,
                 field=None, parent=None):
        self._reference_document_class = reference_document_class
        self.__parent__ = parent
        self._field = field
        self._ids = ids or []
        self._documents = []

        if not ids:
            self._resolved = True

    def __repr__(self):
        if self._resolved:
            items = ', '.join([repr(d) for d in self._documents])
        else:
            items = ', '.join([str(i) for i in self._ids])

        if len(items) > 200:  # pragma: nocover
            items = items[:75] + ' ... ' + items[-75:]

        return "{cname}({rdc} {resolved}{changed} {len} [{items}])".format(
            cname=self.__class__.__name__,
            rdc=self._reference_document_class.__name__,
            resolved='r' if self.resolved else '',
            changed='c' if self.changed else '',
            len=len(self),
            items=items,
        )

    def __getitem__(self, idx: int):
        self._check_resolved_and_rise()
        return self._documents[idx]

    def __setitem__(self, idx: int, document: 'Document'):
        self._check_resolved_and_rise()
        self._set_changed()
        self._ids[idx] = document
        self._documents[idx] = document

    def __delitem__(self, idx: int):
        self._check_resolved_and_rise()
        self._set_changed()
        del self._ids[idx]
        del self._documents[idx]

    def __len__(self):
        return len(self._ids)

    def __bool__(self):
        return bool(self._ids)

    def __iter__(self):
        self._check_resolved_and_rise()
        return iter(self._documents)

    def __eq__(self, other):  # pragma: nocover
        if isinstance(other, ReferencesList):
            return self._ids == other._ids
        else:
            return NotImplemented

    @property
    def resolved(self) -> bool:
        return self._resolved

    @property
    def changed(self) -> bool:
        return self._changed

    @property
    def ids(self) -> list:
        return self._ids.copy()

    def insert(self, idx: int, document: 'Document'):
        self._check_resolved_and_rise()
        self._set_changed()
        self._ids.insert(idx, document.id)
        self._documents.insert(idx, document)

    def append(self, document: 'Document'):
        self._check_resolved_and_rise()
        self._set_changed()
        self._ids.append(document.id)
        self._documents.append(document)

    def pop(self, idx: int=-1):
        self._check_resolved_and_rise()
        self._set_changed()
        del self._ids[idx]
        return self._documents.pop(idx)

    def resolve(self):
        """ Resolve ids to documents.

        This method can be used with "await".
        """
        if self._resolved:
            raise AlreadyResolved()

        db = self.__db__
        qs = db.get_queryset(self._reference_document_class)

        if not db.aio:
            self._documents = list(qs.find_in(
                self._ids,
                not_found=NotFoundBehavior.NONE
            ))
            self._resolved = True
        else:
            async def resolver_coro(self):
                accumulator = []
                async for doc in qs.find_in(self._ids):
                    accumulator.append(doc)

                self._documents = accumulator
                self._resolved = True

            return resolver_coro(self)

    def _check_resolved_and_rise(self):
        if not self._resolved:
            raise NotResolved()

    def _set_changed(self):
        if not self._changed:
            self._changed = True

            # if self.__parent__ is not None:
            #     self._field.set_parent_changed(self)


class ReferencesListField(Field):
    def __init__(self, reference_document_class):
        self._reference_document_class = reference_document_class

    def copy(self):  # pragma: no cover
        return self.__class__()

    def get_if_attribute_not_set(self, document):  # pragma: no cover
        return ReferencesList(
            self._reference_document_class, [],
            field=self,
            parent=document,
        )

    def get_default(self, document):
        return ReferencesList(
            self._reference_document_class, [],
            field=self,
            parent=document,
        )

    def prepare_value(self, document, value):
        if isinstance(value, ReferencesList):
            value._field = self
            value.__parent__ = document
            self.set_parent_changed(value)
            return value

        elif isinstance(value, list):
            ids = [i.id for i in value]
            res = ReferencesList(
                self._reference_document_class, ids,
                field=self,
                parent=document,
            )
            res._resolved = True
            res._documents = value
            self.set_parent_changed(res)
            return res

        else:  # pragma: no cover
            raise TypeError(value)

    def from_mongo(self, document, value: list):
        return ReferencesList(
            self._reference_document_class, value,
            field=self,
            parent=document,
        )

    def to_mongo(self, document, value: ReferencesList):
        return value._ids
