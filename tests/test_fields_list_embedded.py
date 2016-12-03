from unittest import SkipTest

import pytest

from yadm import fields
from yadm.documents import Document, EmbeddedDocument


class EDoc(EmbeddedDocument):
    i = fields.IntegerField()
    s = fields.StringField()


class Doc(Document):
    __collection__ = 'docs'
    li = fields.ListField(fields.EmbeddedDocumentField(EDoc))


def test_save(db):
    doc = Doc()

    edoc = EDoc()
    edoc.i = 13
    doc.li.append(edoc)

    assert edoc.__parent__ is doc.li

    edoc = EDoc()
    edoc.i = 42
    doc.li.append(edoc)

    db.insert(doc)

    data = db.db.docs.find_one()

    assert 'li' in data
    assert len(data['li']) == 2
    assert 'i' in data['li'][0]
    assert data['li'][0]['i'] == 13
    assert data['li'][1]['i'] == 42


def test_load(db):
    db.db.docs.insert({'li': [{'i': 13}, {'i': 42}]})

    doc = db.get_queryset(Doc).find_one()

    assert hasattr(doc, 'li')
    assert len(doc.li) == 2
    assert doc.li[0].__parent__ is doc.li
    assert doc.li[1].__parent__ is doc.li

    for item in doc.li:
        assert isinstance(item, EDoc)

    assert doc.li[0].i == 13
    assert doc.li[1].i == 42


def test_push_reload(db):
    doc = Doc()
    db.insert(doc)

    edoc = EDoc()
    edoc.i = 13
    doc.li.push(edoc)

    db.db.docs.update({}, {'$push': {'li': {'i': 32}}})
    assert len(doc.li) == 1

    edoc = EDoc()
    edoc.i = 42
    doc.li.push(edoc)

    assert len(doc.li) == 3
    assert doc.li[0].i == 13
    assert doc.li[1].i == 32
    assert doc.li[2].i == 42

    data = db.db.docs.find_one()

    assert data['li'][0]['i'] == 13
    assert data['li'][1]['i'] == 32
    assert data['li'][2]['i'] == 42


def test_push_no_reload(db):
    doc = Doc()
    db.insert(doc)

    edoc = EDoc()
    edoc.i = 13
    doc.li.push(edoc)

    db.db.docs.update({}, {'$push': {'li': {'i': 32}}})
    assert len(doc.li) == 1

    edoc = EDoc()
    edoc.i = 42
    doc.li.push(edoc, reload=False)

    assert len(doc.li) == 2
    assert doc.li[0].i == 13
    assert doc.li[1].i == 42

    data = db.db.docs.find_one()

    assert data['li'][0]['i'] == 13
    assert data['li'][1]['i'] == 32
    assert data['li'][2]['i'] == 42


def test_replace(db):
    doc = Doc()
    doc.li.append(EDoc(i=13, s='13'))
    doc.li.append(EDoc(i=42, s='42'))
    db.insert(doc)

    doc.li.replace({'i': 13}, EDoc(i=26))

    data = db.db.docs.find_one()

    assert 'li' in data
    assert len(data['li']) == 2
    assert 'i' in data['li'][0]
    assert data['li'][0]['i'] == 26
    assert 's' not in data['li'][0]
    assert data['li'][1]['i'] == 42
    assert data['li'][1]['s'] == '42'


def test_update(db):
    doc = Doc()
    doc.li.append(EDoc(i=13, s='13'))
    doc.li.append(EDoc(i=42, s='42'))
    db.insert(doc)

    doc.li.update({'i': 13}, {'i': 26})

    data = db.db.docs.find_one()

    assert 'li' in data
    assert len(data['li']) == 2
    assert 'i' in data['li'][0]
    assert data['li'][0]['i'] == 26
    assert data['li'][0]['s'] == '13'
    assert data['li'][1]['i'] == 42
    assert data['li'][1]['s'] == '42'


class DeepEEDoc(EmbeddedDocument):
    i = fields.IntegerField()
    s = fields.StringField()


class DeepEDoc(EmbeddedDocument):
    lie = fields.ListField(fields.EmbeddedDocumentField(DeepEEDoc))


class DeepDoc(Document):
    __collection__ = 'docs'
    li = fields.ListField(fields.EmbeddedDocumentField(DeepEDoc))


def test_deep_save(db):
    doc = DeepDoc()
    doc.li.append(DeepEDoc())

    eedoc = DeepEEDoc()
    eedoc.i = 13
    doc.li[0].lie.append(eedoc)

    db.save(doc)

    data = db.db.docs.find_one()

    assert 'li' in data
    assert len(data['li']) == 1
    assert 'lie' in data['li'][0]
    assert len(data['li'][0]['lie']) == 1
    assert 'i' in data['li'][0]['lie'][0]
    assert data['li'][0]['lie'][0]['i'] == 13


def test_deep_load(db):
    db.db.docs.insert({'li': [{'lie': [{'i': 13}]}]})

    doc = db.get_queryset(DeepDoc).find_one()

    assert hasattr(doc, 'li')
    assert len(doc.li) == 1
    assert hasattr(doc.li[0], 'lie')
    assert len(doc.li[0].lie) == 1
    assert hasattr(doc.li[0].lie[0], 'i')
    assert doc.li[0].lie[0].i == 13


def test_deep_push(db):
    doc = DeepDoc()
    db.insert(doc)

    doc.li.push(DeepEDoc())

    eedoc = DeepEEDoc()
    eedoc.i = 13
    doc.li[0].lie.push(eedoc, reload=False)
    db.reload(doc)

    data = db.db.docs.find_one()

    assert 'li' in data
    assert len(data['li']) == 1
    assert 'lie' in data['li'][0]
    assert len(data['li'][0]['lie']) == 1
    assert 'i' in data['li'][0]['lie'][0]
    assert data['li'][0]['lie'][0]['i'] == 13


def test_deep_push_valueerror(db):
    doc = DeepDoc()
    db.insert(doc)

    doc.li.push(DeepEDoc())

    eedoc = DeepEEDoc()
    eedoc.i = 13

    with pytest.raises(ValueError):
        doc.li[0].lie.push(eedoc)


def test_deep_pull(db):
    _id = db.db.docs.insert({'li': [{'lie': [{'i': 13}, {'i': 42}]}]})
    doc = db.get_queryset(DeepDoc).find_one({'_id': _id})

    doc.li[0].lie.pull({'i': 42}, reload=False)
    db.reload(doc)

    data = db.db.docs.find_one({'_id': _id})

    assert 'li' in data
    assert len(data['li']) == 1
    assert 'lie' in data['li'][0]
    assert len(data['li'][0]['lie']) == 1
    assert 'i' in data['li'][0]['lie'][0]
    assert data['li'][0]['lie'][0]['i'] == 13
