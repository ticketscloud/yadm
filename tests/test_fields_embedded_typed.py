import pytest

from yadm import fields
from yadm.documents import Document, EmbeddedDocument
from yadm.testing import create_fake


class EDocOne(EmbeddedDocument):
    type = fields.StringField(default='one')
    i = fields.IntegerField()


class EDocTwo(EmbeddedDocument):
    type = fields.StringField(default='two')
    s = fields.StringField()


class Doc(Document):
    __collection__ = 'test'
    e = fields.TypedEmbeddedDocumentField(
        type_field='type',
        types={'one': EDocOne,
               'two': EDocTwo})

    l = fields.ListField(
        fields.TypedEmbeddedDocumentField(
            type_field='type',
            types={'one': EDocOne,
                   'two': EDocTwo},
        ))


@pytest.mark.parametrize('type_name, ed_class', [
    ('one', EDocOne),
    ('two', EDocTwo),
])
def test_load(db, type_name, ed_class):
    db.db.test.insert({'e': {'type': type_name}})
    doc = db(Doc).find_one()
    assert isinstance(doc.e, ed_class)


def test_load_bad_type(db):
    db.db.test.insert({'e': {'type': 'bad_type'}})
    doc = db(Doc).find_one()

    with pytest.raises(ValueError):
        doc.e


@pytest.mark.parametrize('ed_class, type_name, data', [
    (EDocOne, 'one', {'i': 1}),
    (EDocTwo, 'two', {'s': '2'}),
])
def test_save(db, ed_class, type_name, data):
    doc = Doc()
    doc.e = ed_class(**data)
    db.save(doc)

    raw = db.db.test.find_one()
    data['type'] = type_name
    assert raw['e'] == data


def test_load_list(db):
    db.db.test.insert({'l': [
        {'type': 'one', 'i': 1},
        {'type': 'two', 'i': '2'},
    ]})

    doc = db(Doc).find_one()
    assert isinstance(doc.l[0], EDocOne)
    assert isinstance(doc.l[1], EDocTwo)


def test_fake():
    doc = create_fake(Doc)
    assert isinstance(doc.e, (EDocOne, EDocTwo))

    if isinstance(doc.e, EDocOne):
        assert isinstance(doc.e.i, int)
    elif isinstance(doc.e, EDocTwo):
        assert isinstance(doc.e.s, str)
    else:
        raise AssertionError(doc.e, type(doc.e))
