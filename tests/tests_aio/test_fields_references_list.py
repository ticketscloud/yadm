import pytest
from yadm.documents import Document
from yadm import fields
from yadm.fields.references_list import ReferencesListField, ReferencesList


class RDoc(Document):
    __collection__ = 'testrdocs'
    i = fields.IntegerField()


class Doc(Document):
    __collection__ = 'testdocs'
    ref = ReferencesListField(RDoc)


@pytest.mark.asyncio
async def test_resolve(db):
    ref_one = RDoc()
    ref_one.i = 13
    await db.insert_one(ref_one)

    ref_two = RDoc()
    ref_two.i = 42
    await db.insert_one(ref_two)

    ref_three = RDoc()
    ref_three.i = 666
    await db.insert_one(ref_three)

    doc = Doc()
    doc.ref.extend([ref_one, ref_two])
    await db.insert_one(doc)

    doc = await db.get_document(Doc, doc.id)

    assert isinstance(doc.ref, ReferencesList)
    assert len(doc.ref) == 2
    assert len(doc.ref.ids) == 2
    assert len(doc.ref._documents) == 0
    assert not doc.ref.resolved

    await doc.ref.resolve()

    assert doc.ref.resolved
    assert len(doc.ref) == 2
    assert len(doc.ref.ids) == 2
    assert len(doc.ref._documents) == 2
