from bson import ObjectId

from yadm import fields
from yadm.documents import Document


class TestRDoc(Document):
    __collection__ = 'testrdocs'
    i = fields.IntegerField()


class TestDoc(Document):
    __collection__ = 'testdocs'
    li = fields.SetField(fields.ReferenceField(TestRDoc))


def test_save(db):
    doc = TestDoc()

    ref_one = TestRDoc()
    ref_one.i = 13
    db.insert(ref_one)
    doc.li.add(ref_one)

    ref_two = TestRDoc()
    ref_two.i = 42
    db.insert(ref_two)
    doc.li.add(ref_two)

    db.insert(doc)

    data = db.db.testdocs.find_one()

    assert 'li' in data
    assert len(data['li']) == 2

    res = set()

    for item in data['li']:
        assert isinstance(item, ObjectId)
        res.add(item)

    assert res == {ref_one.id, ref_two.id}


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

    res = set()

    for item in doc.li:
        assert isinstance(item, TestRDoc)
        assert hasattr(item, 'i')
        assert isinstance(item.i, int)
        res.add(item.i)

    assert res == {13, 42}
