from bson import ObjectId

from yadm import fields
from yadm.documents import Document


class TestRDoc(Document):
    __collection__ = 'testrdocs'
    i = fields.IntegerField()


class TestDoc(Document):
    __collection__ = 'testdocs'
    li = fields.ListField(fields.ReferenceField(TestRDoc))


def test_save(db):
    doc = TestDoc()

    ref_one = TestRDoc()
    ref_one.i = 13
    db.insert(ref_one)
    doc.li.append(ref_one)

    ref_two = TestRDoc()
    ref_two.i = 42
    db.insert(ref_two)
    doc.li.append(ref_two)

    db.insert(doc)

    data = db.db.testdocs.find_one()

    assert len(data['li']) == 2
    assert isinstance(data['li'][0], ObjectId)
    assert isinstance(data['li'][1], ObjectId)
    assert data['li'][0] == ref_one.id
    assert data['li'][1] == ref_two.id


def test_load(db):
    db.db.testdocs.insert(
        {'li': [
            db.db.testrdocs.insert({'i': 13}),
            db.db.testrdocs.insert({'i': 42}),
        ]}
    )

    doc = db.get_queryset(TestDoc).find_one()

    assert hasattr(doc, 'li')
    assert len(doc.li) == 2

    for item in doc.li:
        assert isinstance(item, TestRDoc)

    assert doc.li[0].i == 13
    assert doc.li[1].i == 42
