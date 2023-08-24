from bson import ObjectId

import pytest
from yadm.documents import Document
from yadm import fields


class DocRefIntId(Document):
    __collection__ = 'testdocs_int_id_ref'
    _id = fields.IntegerField()


class DocRef(Document):
    __collection__ = 'testdocs_ref'
    i = fields.IntegerField()


class Doc(Document):
    __collection__ = 'testdocs'
    ref = fields.ReferenceField(DocRef)
    ref_int = fields.ReferenceField(DocRefIntId)


@pytest.mark.asyncio
async def test_get(db):
    id_ref = (await db.db.testdocs_ref.insert_one({'i': 13})).inserted_id
    id = (await db.db.testdocs.insert_one({'ref': id_ref})).inserted_id

    doc = await db.get_queryset(Doc).find_one(id)

    assert ' -)' in repr(doc.ref)
    ref = await doc.ref
    assert ' +)' in repr(doc.ref)

    assert doc._id == id
    assert isinstance(ref, Document)
    assert ref._id == id_ref
    assert isinstance(ref, DocRef)
    assert ref is doc.ref.document
    assert doc.ref is doc.ref
    assert doc.__qs__.cache[(DocRef, ref.id)] is doc.ref


@pytest.mark.asyncio
async def test_get_reference_from_new_instance(db):
    _id = (await db.db.testdocs_ref.insert_one({'i': 13})).inserted_id
    doc_ref = await db(DocRef).find_one({'_id': _id})

    doc = Doc()
    doc.ref = doc_ref

    await db.insert_one(doc)

    assert (await doc.ref).id == _id


@pytest.mark.asyncio
async def test_get_by_ref_int_id(db):
    id_ref = (await db.db.testdocs_int_id_ref.insert_one({'_id': 1})).inserted_id
    id = (await db.db.testdocs.insert_one({'ref_int': id_ref})).inserted_id

    doc = await db.get_queryset(Doc).find_one(id)
    ref = await doc.ref_int
    assert doc._id == id
    assert isinstance(ref, Document)
    assert doc.ref_int._id == id_ref == ref.id
