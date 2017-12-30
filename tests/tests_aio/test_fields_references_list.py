from yadm.documents import Document
from yadm import fields
from yadm.fields.references_list import ReferencesListField, ReferencesList


class RDoc(Document):
    __collection__ = 'testrdocs'
    i = fields.IntegerField()


class Doc(Document):
    __collection__ = 'testdocs'
    ref = ReferencesListField(RDoc)


def test_resolve(loop, db):
    async def test():
        ref_one = RDoc()
        ref_one.i = 13
        await db.insert(ref_one)

        ref_two = RDoc()
        ref_two.i = 42
        await db.insert(ref_two)

        ref_three = RDoc()
        ref_three.i = 666
        await db.insert(ref_three)

        doc = Doc()
        doc.ref.extend([ref_one, ref_two])
        await db.insert(doc)

        doc = await db.get_document(Doc, doc.id)

        assert isinstance(doc.ref, ReferencesList)
        assert len(doc.ref) == 2
        assert len(doc.ref.ids) == 2
        assert len(doc.ref._documents) == 0
        assert not doc.ref.resolved
        assert not doc.ref.changed

        await doc.ref.resolve()

        assert doc.ref.resolved
        assert not doc.ref.changed
        assert len(doc.ref) == 2
        assert len(doc.ref.ids) == 2
        assert len(doc.ref._documents) == 2

    loop.run_until_complete(test())
