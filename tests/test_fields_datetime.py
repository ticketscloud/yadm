from datetime import datetime

import pytz

from yadm import fields
from yadm.documents import Document


class TestDoc(Document):
    __collection__ = 'testdocs'
    dt = fields.DatetimeField()
    now = fields.DatetimeField(auto_now=True)


def test_cast():
    doc = TestDoc()
    doc.dt = datetime(1970, 1, 1, tzinfo=pytz.utc).isoformat()
    assert isinstance(doc.dt, datetime)


def test_default_none():
    doc = TestDoc()
    assert not hasattr(doc, 'dt')


def test_save(db):
    doc = TestDoc()
    doc.dt = datetime(1970, 1, 1, tzinfo=pytz.utc)
    db.insert(doc)

    doc = db.get_queryset(TestDoc).with_id(doc.id)
    data = db.db.testdocs.find_one()

    assert isinstance(data['dt'], datetime)
    assert data['dt'] == doc.dt


def test_load(db):
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
    _id = db.db.testdocs.insert({'dt': epoch})

    doc = db.get_queryset(TestDoc).with_id(_id)

    assert isinstance(doc.dt, datetime)
    assert doc.dt == epoch


def test_default_auto_now():
    doc = TestDoc()
    assert hasattr(doc, 'now')
    assert isinstance(doc.now, datetime)


def test_default_now_save(db):
    doc = TestDoc()
    db.insert(doc)

    doc = db.get_queryset(TestDoc).with_id(doc.id)
    data = db.db.testdocs.find_one()

    assert isinstance(data['now'], datetime)
    assert data['now'] == doc.now


def test_now_save(db):
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

    doc = TestDoc()
    doc.now = epoch
    db.insert(doc)

    doc = db.get_queryset(TestDoc).with_id(doc.id)
    data = db.db.testdocs.find_one()

    assert isinstance(data['now'], datetime)
    assert data['now'] == epoch


def test_now_load(db):
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
    _id = db.db.testdocs.insert({'now': epoch})

    doc = db.get_queryset(TestDoc).with_id(_id)

    assert isinstance(doc.now, datetime)
    assert doc.now == epoch
