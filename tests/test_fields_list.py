import pytest

from yadm import fields
from yadm.documents import Document


class Doc(Document):
    __collection__ = 'testdoc'
    li = fields.ListField(fields.IntegerField())


def test_default():
    doc = Doc()
    assert isinstance(doc.li, fields.list.List)
    assert not doc.li
    assert len(doc.li) == 0
    assert doc.li._data == []
    assert doc.li == []


def test_default_no_auto():
    class Doc(Document):
        __collection__ = 'testdoc'
        li = fields.ListField(fields.IntegerField(), auto_create=False)

    doc = Doc()
    assert not hasattr(doc, 'li')


def test_get(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)

    assert doc.li
    assert len(doc.li) == 3
    assert doc.li._data == [1, 2, 3]
    assert list(doc.li) == [1, 2, 3]
    assert doc.li[1] == 2


def test_append(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)
    doc.li.append(4)
    assert doc.li == [1, 2, 3, 4]


def test_append_valueerror():
    doc = Doc()
    with pytest.raises(ValueError):
        doc.li.append('not a number')


def test_append_save(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)
    doc.li.append(4)
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['li'] == [1, 2, 3, 4]


def test_remove(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)
    doc.li.remove(2)

    assert doc.li == [1, 3]


def test_remove_save(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)
    doc.li.remove(2)
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['li'] == [1, 3]


def test_push(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)
    doc.li.push(4)

    assert doc.li == [1, 2, 3, 4]

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['li'] == [1, 2, 3, 4]


def test_push_valueerror():
    doc = Doc()
    with pytest.raises(ValueError):
        doc.li.push('not a number')


def test_pull(db):
    _id = db.db.testdoc.insert_one({'li': [1, 2, 3]}).inserted_id
    doc = db.get_queryset(Doc).find_one(_id)
    doc.li.pull(2)

    assert doc.li == [1, 3]

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['li'] == [1, 3]
