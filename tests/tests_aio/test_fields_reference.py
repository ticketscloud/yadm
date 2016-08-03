import pytest

from bson import ObjectId

from yadm.documents import Document
from yadm import fields


class DocRef(Document):
    __collection__ = 'testdocs_ref'
    i = fields.IntegerField()


class Doc(Document):
    __collection__ = 'testdocs'
    ref = fields.ReferenceField(DocRef)


def test_get(loop, db):
    async def test():
        id_ref = await db.db.testdocs_ref.insert({'i': 13})
        id = await db.db.testdocs.insert({'ref': id_ref})

        doc = await db.get_queryset(Doc).find_one(id)

        assert ' -)' in repr(doc.ref)
        ref = await doc.ref
        assert ' +)' in repr(doc.ref)

        assert doc._id == id
        assert isinstance(ref, Document)
        assert ref._id == id_ref
        assert isinstance(doc.ref, ObjectId)
        assert ref is (await doc.ref) is (await doc.ref) is doc.ref.document
        assert doc.ref is doc.ref
        assert doc.__qs__.cache[(DocRef, ref.id)] is doc.ref

    loop.run_until_complete(test())
