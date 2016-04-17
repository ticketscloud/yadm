import pytest
from bson import ObjectId

from yadm.documents import Document
from yadm.markers import AttributeNotSet
from yadm import fields


class TestDoc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField()
    b = fields.BooleanField()


@pytest.fixture
def doc():
    return TestDoc()


def test__db(doc):
    assert doc.__db__ is None


def test_fields():
    assert set(TestDoc.__fields__) == {'_id', 'i', 'b'}


def test_inheritance_fields():
    class InhTestDoc(TestDoc):
        d = fields.DecimalField

    assert set(InhTestDoc.__fields__) == {'_id', 'i', 'b', 'd'}
    assert set(TestDoc.__fields__) == {'_id', 'i', 'b'}
    assert TestDoc.__fields__['i'] is not InhTestDoc.__fields__['i']
    assert TestDoc.__fields__['i'].document_class is TestDoc
    assert InhTestDoc.__fields__['i'].document_class is InhTestDoc


def test_changed(doc):
    assert not doc.__raw__
    assert not doc.__cache__
    assert not doc.__changed__

    doc.i = 13
    assert not doc.__raw__
    assert not doc.__cache__
    assert doc.__changed__ == {'i': 13}

    doc.b = True
    assert not doc.__raw__
    assert not doc.__cache__
    assert doc.__changed__ == {'i': 13, 'b': True}


def test_changed_clear(doc):
    doc.i = 13
    doc.__changed_clear__()
    assert not doc.__raw__
    assert doc.__cache__ == {'i': 13}
    assert not doc.__changed__


def test_raw_cache_changed(db, doc):
    assert not doc.__raw__
    assert not doc.__cache__
    assert not doc.__changed__

    doc.i = 13

    assert not doc.__raw__
    assert not doc.__cache__
    assert doc.__changed__ == {'i': 13}

    db.save(doc)

    assert not doc.__raw__
    assert doc.__cache__ == {'i': 13, '_id': doc.id, 'b': AttributeNotSet}
    assert not doc.__changed__

    doc = db.reload(doc, new_instance=True)

    assert doc.__raw__ == {'i': 13, '_id': doc.id, 'b': AttributeNotSet}
    assert doc.__cache__ == {'_id': doc.id}
    assert not doc.__changed__

    doc.b = False

    assert doc.__raw__ == {'i': 13, '_id': doc.id, 'b': AttributeNotSet}
    assert doc.__cache__ == {'_id': doc.id}
    assert doc.__changed__ == {'b': False}

    assert doc.i == 13  # call descriptor's get

    assert doc.__raw__ == {'i': 13, '_id': doc.id, 'b': AttributeNotSet}
    assert doc.__cache__ == {'i': 13, '_id': doc.id}
    assert doc.__changed__ == {'b': False}

    doc.i = 12  # call descriptor's set, for cover

    assert doc.__raw__ == {'i': 13, '_id': doc.id, 'b': AttributeNotSet}
    assert doc.__cache__ == {'i': 13, '_id': doc.id}
    assert doc.__changed__ == {'i': 12, 'b': False}


def test_eq():
    doc_a = TestDoc()
    doc_a.id = ObjectId()

    doc_b = TestDoc()
    doc_b.id = ObjectId()

    doc_c = TestDoc()
    doc_c.id = doc_a.id

    assert doc_a != doc_b
    assert doc_a == doc_c


def test_id(doc):
    with pytest.raises(AttributeError):
        doc.id


def test_attributeerror(db):
    _id = db.db.testdocs.insert({'i': 13})
    doc = db(TestDoc).find_one(_id)

    assert doc.i == 13

    with pytest.raises(AttributeError):
        doc.b


def test_notloaded(db):
    _id = db.db.testdocs.insert({'i': 13, 'b': False})
    doc = db(TestDoc).fields('i').find_one(_id)

    assert doc.i == 13

    with pytest.raises(fields.NotLoadedError):
        doc.b
