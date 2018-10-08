""" Field for list with references.

Usage:

    class Doc(Document):
        refs = ReferencesListField(RefDoc)


    doc = db(Doc).find_one(...)
    doc.ref.resolve()  # resolve all documents in with one query
    for ref_doc in doc.ref:
        ...

With asyncio:

    await doc.ref.resolve()

doc.ref is a ReferencesList instance, witch subclass MutableSequence.
But without resolving NotResolved raised for any actions with it
(except __len__ and __bool__).

"""
from collections.abc import MutableSequence
from typing import (
    NamedTuple,
    Any,
    Optional,
    Union,
    List,
    Iterator,
    Coroutine,
)

from bson import ObjectId

from yadm.documents import MetaDocument, BaseDocument, Document
from yadm.document_item import DocumentItemMixin
from yadm.queryset import NotFoundBehavior
from yadm.fields.base import Field


class NotResolved(Exception):
    pass


class AlreadyResolved(Exception):
    pass


class ReferencesListSetitem(NamedTuple):
    index: int
    document: Document
    op: str = 'references_list_setitem'


class ReferencesListDelitem(NamedTuple):
    index: int
    op: str = 'references_list_delitem'


class ReferencesListInsert(NamedTuple):
    index: int
    document: Document
    op: str = 'references_list_insert'


class ReferencesListAppend(NamedTuple):
    document: Document
    op: str = 'references_list_append'


class ReferencesListPop(NamedTuple):
    index: int
    op: str = 'references_list_pop'


class ReferencesListResolve(NamedTuple):
    op: str = 'references_list_resolve'


class ReferencesList(MutableSequence, DocumentItemMixin):
    _resolved = False

    def __init__(self,
                 reference_document_class: MetaDocument,
                 ids: Optional[List[ObjectId]] = None,
                 field: Optional[Field] = None,
                 parent: Union[BaseDocument, DocumentItemMixin, None] = None):
        super().__init__()
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

        return "{cname}({rdc} {resolved} {len} [{items}])".format(
            cname=self.__class__.__name__,
            rdc=self._reference_document_class.__name__,
            resolved='resolved' if self.resolved else '',
            len=len(self),
            items=items,
        )

    def __getitem__(self, idx: int) -> Document:
        self._check_resolved_and_rise()
        return self._documents[idx]

    def __setitem__(self, idx: int, document: Document):
        self._check_resolved_and_rise()
        self._ids[idx] = document
        self._documents[idx] = document
        self.__log__.append(ReferencesListSetitem(index=idx,
                                                  document=document))

    def __delitem__(self, idx: int):
        self._check_resolved_and_rise()
        del self._ids[idx]
        del self._documents[idx]
        self.__log__.append(ReferencesListDelitem(index=idx))

    def __len__(self) -> int:
        return len(self._ids)

    def __bool__(self) -> bool:
        return bool(self._ids)

    def __iter__(self) -> Iterator:
        self._check_resolved_and_rise()
        return iter(self._documents)

    def __eq__(self, other) -> bool:  # pragma: nocover
        if isinstance(other, ReferencesList):
            return self._ids == other._ids
        else:
            return NotImplemented

    @property
    def resolved(self) -> bool:
        return self._resolved

    @property
    def ids(self) -> list:
        return self._ids.copy()

    def insert(self, idx: int, document: Document):
        self._check_resolved_and_rise()
        self._ids.insert(idx, document.id)
        self._documents.insert(idx, document)
        self.__log__.append(ReferencesListInsert(index=idx, document=document))

    def append(self, document: Document):
        self._check_resolved_and_rise()
        self._ids.append(document.id)
        self._documents.append(document)
        self.__log__.append(ReferencesListAppend(document=document))

    def pop(self, idx: int=-1) -> Document:
        self._check_resolved_and_rise()
        del self._ids[idx]
        doc = self._documents.pop(idx)
        self.__log__.append(ReferencesListPop(index=idx))
        return doc

    def resolve(self) -> Optional[Coroutine]:
        """ Resolve ids to documents.

        This method can be used with "await" if AioDatabase.
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
            self.__log__.append(ReferencesListResolve())

        else:
            async def resolver_coro(self) -> None:
                documents = []
                async for doc in qs.find_in(self._ids):
                    documents.append(doc)

                self._documents = documents
                self._resolved = True
                self.__log__.append(ReferencesListResolve())

            return resolver_coro(self)

    def _check_resolved_and_rise(self):
        if not self._resolved:
            raise NotResolved()


class ReferencesListField(Field):
    def __init__(self, reference_document_class):
        self._reference_document_class = reference_document_class

    def copy(self) -> 'ReferencesListField':  # pragma: no cover
        return self.__class__()

    def get_if_attribute_not_set(
        self,
        document: Document,
    ) -> ReferencesList:  # pragma: no cover
        rl = ReferencesList(
            self._reference_document_class, [],
            field=self,
            parent=document,
        )
        setattr(document, self.name, rl)
        return rl

    def get_default(self, document: Document) -> ReferencesList:
        rl = ReferencesList(
            self._reference_document_class, [],
            field=self,
            parent=document,
        )
        setattr(document, self.name, rl)
        return rl

    def prepare_value(
        self,
        document: Document,
        value: Union[ReferencesList, List[Document]],
    ) -> ReferencesList:
        if isinstance(value, ReferencesList):
            value._field = self
            value.__parent__ = document
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
            return res

        else:  # pragma: no cover
            raise TypeError(value)

    def from_mongo(self,
                   document: Document,
                   value: list) -> ReferencesList:
        return ReferencesList(
            self._reference_document_class, value,
            field=self,
            parent=document,
        )

    def to_mongo(self,
                 document: Document,
                 value: ReferencesList) -> List[ObjectId]:
        return value._ids
