import pytest
from bson import ObjectId

from yadm import fields
from yadm.documents import Document, EmbeddedDocument
from yadm.testing import create_fake


class EDoc(EmbeddedDocument):
    id = fields.ObjectIdField(default_gen=True)
    i = fields.IntegerField()
    s = fields.StringField(default='default')
    e = fields.EmbeddedDocumentField('self')


class Doc(Document):
    __collection__ = 'testdoc'
    e = fields.EmbeddedDocumentField(EDoc, auto_create=False)


class DocAuto(Document):
    __collection__ = 'testdoc'
    e = fields.EmbeddedDocumentField(EDoc)


def test_default():
    doc = Doc()
    assert not hasattr(doc, 'e')


def test_default_auto():
    doc = DocAuto()
    assert hasattr(doc, 'e')
    assert isinstance(doc.e, EDoc)


def test_default_auto_not_save_empty(db):
    class ETD(EmbeddedDocument):
        i = fields.IntegerField()

    class TD(Document):
        __collection__ = 'testdoc'
        e = fields.EmbeddedDocumentField(ETD)

    doc = db.save(TD())
    raw = db.db.testdoc.find_one(doc.id)
    assert 'e' not in raw
    assert set(raw) == {'_id'}

    doc.e.i = 13
    db.save(doc)
    raw = db.db.testdoc.find_one(doc.id)
    assert 'e' in raw
    assert raw == {'_id': doc.id, 'e': {'i': 13}}


@pytest.mark.parametrize('raw', [
    {'i': 13},
    {'i': 13, 's': 'defined'},
    {'s': 'defined'},
    {},
    None,
])
def test_get(db, raw):
    if raw is not None:
        _id = db.db.testdoc.insert({'e': raw})
    else:
        _id = db.db.testdoc.insert({})

    doc = db.get_queryset(Doc).find_one(_id)

    if raw is None:
        assert not hasattr(doc, 'e')
    else:
        assert hasattr(doc, 'e')
        assert isinstance(doc.e, EDoc)

        if 'i' in raw:
            assert hasattr(doc.e, 'i')
            assert isinstance(doc.e.i, int)
            assert doc.e.i == raw['i']
        else:
            assert not hasattr(doc.e, 'i')

        if 's' in raw:
            assert hasattr(doc.e, 's')
            assert isinstance(doc.e.s, str)
            assert doc.e.s == raw['s']
        else:
            assert not hasattr(doc.e, 's')


@pytest.mark.parametrize('raw', [
    {'i': 13},
    {'i': 13, 's': 'defined'},
    {'s': 'defined'},
    {},
    None,
])
def test_get_deeper(db, raw):
    if raw is not None:
        _id = db.db.testdoc.insert({'e': {'e': raw}})
    else:
        _id = db.db.testdoc.insert({'e': {}})

    doc = db.get_queryset(Doc).find_one(_id)

    if raw is None:
        assert hasattr(doc.e, 'e')
        assert not hasattr(doc.e.e, 'i')
        assert doc.e.e.s == 'default'
    else:
        assert hasattr(doc.e, 'e')
        assert isinstance(doc.e.e, EDoc)

        if 'i' in raw:
            assert hasattr(doc.e.e, 'i')
            assert isinstance(doc.e.e.i, int)
            assert doc.e.e.i == raw['i']
        else:
            assert not hasattr(doc.e.e, 'i')

        if 's' in raw:
            assert hasattr(doc.e.e, 's')
            assert isinstance(doc.e.e.s, str)
            assert doc.e.e.s == raw['s']
        else:
            assert not hasattr(doc.e.e, 's')


def test_set():
    doc = Doc()
    doc.e = EDoc()

    assert hasattr(doc, 'e')
    assert isinstance(doc.e, EDoc)
    assert not hasattr(doc.e, 'i')

    doc.e.i = 13
    assert hasattr(doc.e, 'i')
    assert isinstance(doc.e.i, int)
    assert doc.e.i == 13


def test_set_typeerror():
    class FailEDoc(EmbeddedDocument):
        s = fields.StringField

    doc = Doc()

    with pytest.raises(TypeError):
        doc.e = FailEDoc()


def test_set_insert_wo_set(db):
    doc = Doc()
    db.insert(doc)

    data = db.db.testdoc.find_one({'_id': doc.id})
    assert set(data) == {'_id'}
    assert data['_id'] == doc.id


def test_set_insert(db):
    doc = Doc()
    doc.e = EDoc()
    doc.e.i = 13
    db.insert(doc)

    data = db.db.testdoc.find_one({'_id': doc.id})
    assert set(data) == {'_id', 'e'}
    assert data['_id'] == doc.id
    assert data['e']['i'] == 13


def test_set_save(db):
    _id = db.db.testdoc.insert({'e': {'i': 13}})
    doc = db.get_queryset(Doc).find_one(_id)

    doc.e.i = 26
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data == {'_id': _id, 'e': {'i': 26}}


def test_delete_save(db):
    _id = db.db.testdoc.insert({'e': {'i': 13}})
    doc = db.get_queryset(Doc).find_one(_id)

    del doc.e
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data == {'_id': _id}


def test_delete_deep_save(db):
    _id = db.db.testdoc.insert({'e': {'i': 13, 'id': ObjectId()}})
    doc = db.get_queryset(Doc).find_one(_id)

    del doc.e.i
    db.save(doc)

    data = db.db.testdoc.find_one({'_id': _id})
    assert data == {'_id': _id, 'e': {'id': doc.e.id}}


def test_id_load_default():
    doc = Doc({'e': {'i': 13}})
    assert hasattr(doc, 'e')
    assert hasattr(doc.e, 'i')
    assert hasattr(doc.e, 'id')
    assert doc.e.i == 13
    assert isinstance(doc.e.id, ObjectId)
    assert doc.e.id is doc.e.id


def test_id_insert(db):
    doc = Doc({'e': {'i': 13}})

    # eid = doc.e.id
    db.insert(doc)
    # assert doc.e.id == eid
    # db.reload(doc)

    data = db.db.testdoc.find_one({'_id': doc.id})
    assert data['e']['id'] == doc.e.id


def test_fake():
    doc = create_fake(Doc)
    assert isinstance(doc.e, EDoc)
    assert isinstance(doc.e.i, int)
    assert isinstance(doc.e.s, str)
    assert isinstance(doc.e.e, EDoc)
