from datetime import datetime

import pytz

from yadm import fields
from yadm.documents import Document


class Doc(Document):
    __collection__ = 'testdocs'
    dt = fields.DatetimeField()
    now = fields.DatetimeField(auto_now=True)


def test_cast():
    doc = Doc()
    doc.dt = datetime(1970, 1, 1, tzinfo=pytz.utc).isoformat()
    assert isinstance(doc.dt, datetime)


def test_default_none():
    doc = Doc()
    assert not hasattr(doc, 'dt')


def test_save(db):
    doc = Doc()
    doc.dt = datetime(1970, 1, 1, tzinfo=pytz.utc)
    db.insert(doc)

    doc = db.get_queryset(Doc).find_one(doc.id)
    data = db.db.testdocs.find_one()

    assert isinstance(data['dt'], datetime)
    assert data['dt'] == doc.dt


def test_load(db):
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
    _id = db.db.testdocs.insert({'dt': epoch})

    doc = db.get_queryset(Doc).find_one(_id)

    assert isinstance(doc.dt, datetime)
    assert doc.dt == epoch


def test_default_auto_now():
    doc = Doc()
    assert hasattr(doc, 'now')
    assert isinstance(doc.now, datetime)


def test_default_now_save(db):
    doc = Doc()
    db.insert(doc)

    doc = db.get_queryset(Doc).find_one(doc.id)
    data = db.db.testdocs.find_one()

    assert isinstance(data['now'], datetime)
    assert data['now'] == doc.now


def test_now_save(db):
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

    doc = Doc()
    doc.now = epoch
    db.insert(doc)

    doc = db.get_queryset(Doc).find_one(doc.id)
    data = db.db.testdocs.find_one()

    assert isinstance(data['now'], datetime)
    assert data['now'] == epoch


def test_now_load(db):
    epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
    _id = db.db.testdocs.insert({'now': epoch})

    doc = db.get_queryset(Doc).find_one(_id)

    assert isinstance(doc.now, datetime)
    assert doc.now == epoch
