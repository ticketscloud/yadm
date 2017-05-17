import pytest

from yadm import fields
from yadm.documents import Document


class Doc(Document):
    __collection__ = 'testdoc'
    e = fields.SimpleEmbeddedDocumentField({
        'i': fields.IntegerField(),
        's': fields.StringField(default='default value'),
    })


def test_init():
    doc = Doc()

    assert not hasattr(doc.e, 'i')
    assert doc.e.s == 'default value'


def test_init__type_error():
    with pytest.raises(TypeError):
        fields.SimpleEmbeddedDocumentField(object())


def test_init__empty():
    with pytest.raises(ValueError):
        fields.SimpleEmbeddedDocumentField({})


def test_save(db):
    doc = Doc()
    doc.e.i = 13
    db.save(doc)

    raw = db.db['testdoc'].find_one()

    assert raw['e']['i'] == 13
    assert raw['e']['s'] == 'default value'


def test_load(db):
    db.db['testdoc'].insert_one({'e': {'i': 26}})

    doc = db(Doc).find_one()

    assert doc.e.i == 26
    assert not hasattr(doc.e, 's')
