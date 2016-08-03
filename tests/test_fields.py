import pytest
from bson import ObjectId

from yadm.documents import Document
from yadm.fields.simple import StringField, IntegerField


class Doc(Document):
    choices = StringField(choices={'qwerty', 'zzz', 'asd'})
    choices_w_default = IntegerField(default=13, choices={0, 13, 42})


def test_choices_set_valid():
    doc = Doc()
    doc.choices = 'zzz'
    assert doc.choices == 'zzz'


def test_choices_set_invalid():
    doc = Doc()

    with pytest.raises(ValueError):
        doc.choices = 'invalid'


def test_choices_w_default_valid():
    doc = Doc()
    assert doc.choices_w_default == 13


def test_choices_w_default_invalid():
    with pytest.raises(ValueError):
        IntegerField(default=1, choices={0, 13, 42})


def test_id_default():
    doc = Doc()

    with pytest.raises(AttributeError):
        doc._id

    with pytest.raises(AttributeError):
        doc.id


def test_id_set():
    _id = ObjectId()
    doc = Doc()
    doc.id = _id

    assert doc.id is _id
    assert doc.id is doc._id


def test_id_set_str():
    _id = ObjectId()
    doc = Doc()
    doc._id = str(_id)

    assert isinstance(doc._id, ObjectId)
    assert doc._id == _id


def test_repr():
    assert '_id' in repr(Doc._id)
