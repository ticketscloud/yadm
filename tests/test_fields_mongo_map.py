from yadm import fields
from yadm.documents import Document


class Doc(Document):
    __collection__ = 'testdoc'
    map = fields.MongoMapField()


def test_load(db):
    _id = db.db.testdoc.insert({'map': {'a': 1, 'b': '2', 'c': 'qwerty'}})
    doc = db.get_queryset(Doc).find_one(_id)

    assert hasattr(doc, 'map')
    assert isinstance(doc.map, fields.UnmutableMap)
    assert len(doc.map) == 3
    assert doc.map == {'a': 1, 'b': '2', 'c': 'qwerty'}


def test_setattr(db):
    _id = db.db.testdoc.insert({'map': {'a': 1}})
    doc = db.get_queryset(Doc).find_one(_id)
    doc.map = {'b': 2}

    assert isinstance(doc.map, fields.UnmutableMap)
    assert doc.map == {'b': 2}


def test_setattr_save(db):
    _id = db.db.testdoc.insert({'map': {'a': 1}})
    doc = db.get_queryset(Doc).find_one(_id)
    doc.map = {'b': 2}

    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data['map'] == {'b': 2}
    assert doc.map == {'b': 2}
