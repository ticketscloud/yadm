import pytest

from yadm import fields
from yadm.documents import Document


class TestDoc(Document):
    __collection__ = 'testdoc'
    s = fields.SetField(fields.IntegerField())


def test_default():
    doc = TestDoc()
    assert isinstance(doc.s, fields.set.Set)
    assert not doc.s
    assert len(doc.s) == 0
    assert doc.s == set()
    assert isinstance(doc.s._data, list)
    assert not doc.s._data


def test_get(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)

    assert doc.s
    assert len(doc.s) == 3
    assert doc.s._data == [1, 2, 3]
    assert doc.s == {1, 2, 3}

    with pytest.raises(TypeError):
        doc.s[1]

    with pytest.raises(TypeError):
        doc.s[1] = 10

    with pytest.raises(TypeError):
        del doc.s[1]


def test_add(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)
    doc.s.add(4)

    assert doc.s == {1, 2, 3, 4}


def test_add_typeerror():
    doc = TestDoc()

    with pytest.raises(ValueError):
        doc.s.add('not a number')


def test_add_save(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)
    doc.s.add(4)
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['s'] == [1, 2, 3, 4]


def test_remove(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)
    doc.s.remove(2)

    assert doc.s == {1, 3}


def test_remove_save(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)
    doc.s.remove(2)
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['s'] == [1, 3]


def test_add_to_set(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)
    doc.s.add_to_set(4)

    assert doc.s == {1, 2, 3, 4}

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['s'] == [1, 2, 3, 4]


def test_add_to_set_typeerror():
    doc = TestDoc()
    with pytest.raises(ValueError):
        doc.s.add_to_set('not a number')


def test_pull(db):
    _id = db.db.testdoc.insert({'s': [1, 2, 3]})
    doc = db.get_queryset(TestDoc).with_id(_id)
    doc.s.pull(2)

    assert doc.s == {1, 3}

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['s'] == [1, 3]
