import pytest

from yadm.documents import Document, EmbeddedDocument
from yadm import fields
from yadm.exceptions import NotLoadedError
from yadm.testing import create_fake
from yadm.serialize import to_mongo, from_mongo


class EDoc(EmbeddedDocument):
    ii = fields.IntegerField()
    ss = fields.StringField()


class Doc(Document):
    __collection__ = 'testdocs'

    i = fields.IntegerField()
    s = fields.StringField()
    e = fields.EmbeddedDocumentField(EDoc)

    extra_class_value = 'value'


def test_to__simple():
    doc = create_fake(Doc)
    doc.extra_doc_value = 'not serialized'
    raw = to_mongo(doc)

    assert raw == {
        '_id': doc.id,
        'i': doc.i,
        's': doc.s,
        'e': {
            'ii': doc.e.ii,
            'ss': doc.e.ss,
        }
    }


def test_to__include():
    doc = create_fake(Doc)
    doc.extra_doc_value = 'not serialized'
    raw = to_mongo(doc, include=['i', 'e.ss'])

    assert raw == {
        'i': doc.i,
        'e': {
            'ss': doc.e.ss,
        }
    }


def test_to__exclude():
    doc = create_fake(Doc)
    doc.extra_doc_value = 'not serialized'
    raw = to_mongo(doc, exclude=['i'])

    assert raw == {
        '_id': doc.id,
        's': doc.s,
        'e': {
            'ii': doc.e.ii,
            'ss': doc.e.ss,
        }
    }


def test_to__not_loaded_error(db):
    doc = create_fake(Doc)
    db.insert_one(doc)

    doc = db(Doc).find_one(doc.id, {'s': False})
    with pytest.raises(NotLoadedError):
        to_mongo(doc)

    doc = db(Doc).find_one(doc.id, {'e.ss': False})
    with pytest.raises(NotLoadedError):
        to_mongo(doc)


def test_to__skip_not_loaded(db):
    doc = create_fake(Doc)
    db.insert_one(doc)

    doc = db(Doc).find_one(doc.id, {'s': False})
    raw = to_mongo(doc, skip_not_loaded=True)
    assert raw == {
        '_id': doc.id,
        'i': doc.i,
        'e': {
            'ii': doc.e.ii,
            'ss': doc.e.ss,
        }
    }

    doc = db(Doc).find_one(doc.id, {'e.ss': False})
    raw = to_mongo(doc, skip_not_loaded=True)
    assert raw == {
        '_id': doc.id,
        'i': doc.i,
        's': doc.s,
        # 'e': {  # TODO: must create part of embedded document
        #     'ii': doc.e.ii,
        # }
    }


def test_from__simple():
    raw = {
        'i': 1,
        's': 'string',
        'e': {
            'ii': 2,
            'ss': 'stringstring',
        },
    }
    doc = from_mongo(Doc, raw)

    assert isinstance(doc, Doc)
    assert doc.i == raw['i']
    assert doc.s == raw['s']

    assert isinstance(doc.e, EDoc)
    assert doc.e.ii == raw['e']['ii']
    assert doc.e.ss == raw['e']['ss']


def test_from__not_loaded():
    raw = {
        'i': 1,
        's': 'string',
        'e': {
            'ii': 2,
            'ss': 'stringstring',
        },
    }
    doc = from_mongo(Doc, raw, not_loaded=['s', 'e.ii'])

    assert isinstance(doc, Doc)
    assert doc.i == raw['i']
    assert doc.__not_loaded__ == frozenset({'s', 'e.ii'})

    assert isinstance(doc.e, EDoc)
    assert doc.e.ss == raw['e']['ss']
    assert doc.e.__not_loaded__ == frozenset({'ii'})


def test_from__smart_null():
    class Doc(Document):
        i = fields.IntegerField(smart_null=True)
        s = fields.StringField()

    doc = from_mongo(Doc, {'s': 'string'})

    assert doc.s == 'string'
    assert doc.i is None
