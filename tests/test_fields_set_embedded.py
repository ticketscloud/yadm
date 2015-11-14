from yadm import fields
from yadm.documents import Document, EmbeddedDocument


class TestEDoc(EmbeddedDocument):
    i = fields.IntegerField()


class TestDoc(Document):
    __collection__ = 'testdocs'
    li = fields.SetField(fields.EmbeddedDocumentField(TestEDoc))


def test_save(db):
    doc = TestDoc()

    edoc = TestEDoc()
    edoc.i = 13
    doc.li.add(edoc)

    edoc = TestEDoc()
    edoc.i = 42
    doc.li.add(edoc)

    db.insert(doc)

    data = db.db.testdocs.find_one()

    assert 'li' in data
    assert len(data['li']) == 2
    assert 'i' in data['li'][0]

    res = set()

    for item in data['li']:
        assert 'i' in item
        assert isinstance(item['i'], int)
        res.add(item['i'])

    assert res == {13, 42}


def test_load(db):
    _id = db.db.testdocs.insert({'li': []})
    db.db.testdocs.update({'_id': _id}, {'$addToSet': {'li': {'i': 13}}})
    db.db.testdocs.update({'_id': _id}, {'$addToSet': {'li': {'i': 42}}})

    doc = db.get_queryset(TestDoc).find_one()

    assert hasattr(doc, 'li')
    assert len(doc.li) == 2

    res = set()

    for item in doc.li:
        assert isinstance(item, TestEDoc)
        assert hasattr(item, 'i')
        assert isinstance(item.i, int)
        res.add(item.i)

    assert res == {13, 42}


def test_add_to_set(db):
    doc = TestDoc()
    db.insert(doc)

    edoc = TestEDoc()
    edoc.i = 13
    doc.li.add_to_set(edoc)

    edoc = TestEDoc()
    edoc.i = 42
    doc.li.add_to_set(edoc)

    data = db.db.testdocs.find_one()

    assert 'li' in data
    assert len(data['li']) == 2
    assert 'i' in data['li'][0]
    assert data['li'][0]['i'] == 13
    assert data['li'][1]['i'] == 42
