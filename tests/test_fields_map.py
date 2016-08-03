import pytest

from bson import ObjectId

from yadm import fields
from yadm.documents import Document


class Doc(Document):
    __collection__ = 'testdoc'
    map = fields.MapField(fields.IntegerField())


def test_default():
    doc = Doc()
    assert isinstance(doc.map, fields.map.Map)
    assert not doc.map
    assert len(doc.map) == 0
    assert doc.map._data == {}
    assert doc.map == {}


def test_load(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)

    assert doc.map
    assert len(doc.map) == 3
    assert doc.map._data == {'a': 1, 'b': 2, 'c': 3}
    assert dict(doc.map) == {'a': 1, 'b': 2, 'c': 3}
    assert doc.map == {'a': 1, 'b': 2, 'c': 3}
    assert doc.map['b'] == 2


def test_getitem_keyerror():
    doc = Doc()
    with pytest.raises(KeyError):
        doc.map['not exist']


def test_setitem(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)
    doc.map['d'] = 4

    assert doc.map == {'a': 1, 'b': 2, 'c': 3, 'd': 4}


def test_setitem_valueerror():
    doc = Doc()
    with pytest.raises(ValueError):
        doc.map['d'] = 'not a number'


def test_setattr_save(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)
    doc.map['d'] = 4
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['map'] == {'a': 1, 'b': 2, 'c': 3, 'd': 4}
    assert doc.map == {'a': 1, 'b': 2, 'c': 3, 'd': 4}


def test_remove(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)
    del doc.map['b']

    assert doc.map == {'a': 1, 'c': 3}


def test_remove_save(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)
    del doc.map['b']
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['map'] == {'a': 1, 'c': 3}


def test_set(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)
    doc.map.set('d', 4)

    assert doc.map == {'a': 1, 'b': 2, 'c': 3, 'd': 4}

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['map'] == {'a': 1, 'b': 2, 'c': 3, 'd': 4}


def test_set_runtimeerror():
    doc = Doc()
    with pytest.raises(RuntimeError):
        doc.map.set('key', 1)


def test_set_valueerror(db):
    doc = Doc()
    db.save(doc)
    with pytest.raises(ValueError):
        doc.map.set('key', 'not a number')


def test_unset(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
    doc = db.get_queryset(Doc).find_one(_id)
    doc.map.unset('b')

    assert doc.map == {'a': 1, 'c': 3}

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['map'] == {'a': 1, 'c': 3}


class DocCustom(Document):
    __collection__ = 'testdoc'
    map = fields.MapCustomKeysField(fields.IntegerField(), ObjectId)


def test_custom_load(db):
    one = ObjectId()
    two = ObjectId()

    _id = db.db.testdoc.insert({'map': {str(one): 1, str(two): 2}})
    doc = db.get_queryset(DocCustom).find_one(_id)

    assert doc.map
    assert len(doc.map) == 2
    assert doc.map._data == {str(one): 1, str(two): 2}
    assert dict(doc.map) == {one: 1, two: 2}
    assert doc.map == {one: 1, two: 2}
    assert doc.map[two] == 2
    assert isinstance(list(doc.map)[0], ObjectId)


def test_custom_setitem(db):
    one = ObjectId()
    two = ObjectId()

    _id = db.db.testdoc.insert({'map': {str(one): 1}})
    doc = db.get_queryset(DocCustom).find_one(_id)

    doc.map[two] = 3
    assert doc.map == {one: 1, two: 3}

    doc.map[two] = 2
    assert doc.map == {one: 1, two: 2}


def test_complex_references(db):
    class RefDoc(Document):
        __collection__ = 'refs'
        i = fields.IntegerField()

    class Doc(Document):
        __collection__ = 'docs'
        refs = fields.MapCustomKeysField(
            fields.ListField(fields.ReferenceField(RefDoc)),
            key_factory=ObjectId)

    _id = ObjectId()
    ref = db.save(RefDoc(i=1))
    doc = db.save(Doc(refs={_id: [ref]}))
    assert doc.refs == {_id: [ref]}

    doc = db(Doc).find_one({'_id': doc.id})
    assert doc.refs == {_id: [ref]}
    assert doc.refs[_id][0] is not ref
