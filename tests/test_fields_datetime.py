from datetime import datetime, date, timedelta

import pytest

import pytz

from yadm import fields
from yadm.documents import Document
from yadm.testing import create_fake


class TestDatetimeField:
    class Doc(Document):
        __collection__ = 'testdocs'
        dt = fields.DatetimeField()
        now = fields.DatetimeField(auto_now=True)

    def test_cast__iso(self):
        doc = self.Doc()
        doc.dt = datetime(1970, 1, 1, tzinfo=pytz.utc).isoformat()
        assert isinstance(doc.dt, datetime)
        assert doc.dt.tzinfo is not None

    def test_cast__date(self):
        doc = self.Doc()
        doc.dt = date(1970, 1, 1)
        assert isinstance(doc.dt, datetime)
        assert doc.dt == datetime(1970, 1, 1, tzinfo=pytz.utc)

    def test_cast__fix_tz(self):
        doc = self.Doc()
        doc.dt = datetime(1970, 1, 1)
        assert isinstance(doc.dt, datetime)
        assert doc.dt.tzinfo == pytz.utc

    def test_default_none(self):
        doc = self.Doc()
        assert not hasattr(doc, 'dt')

    def test_save(self, db):
        doc = self.Doc()
        doc.dt = datetime(1970, 1, 1, tzinfo=pytz.utc)
        db.insert_one(doc)

        doc = db.get_queryset(self.Doc).find_one(doc.id)
        data = db.db.testdocs.find_one()

        assert isinstance(data['dt'], datetime)
        assert data['dt'] == doc.dt

    def test_load(self, db):
        epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
        _id = db.db.testdocs.insert_one({'dt': epoch}).inserted_id

        doc = db.get_queryset(self.Doc).find_one(_id)

        assert isinstance(doc.dt, datetime)
        assert doc.dt == epoch

    def test_default_auto_now(self):
        doc = self.Doc()
        assert hasattr(doc, 'now')
        assert isinstance(doc.now, datetime)

    def test_default_now_save(self, db):
        doc = self.Doc()
        db.insert_one(doc)

        doc = db.get_queryset(self.Doc).find_one(doc.id)
        data = db.db.testdocs.find_one()

        assert isinstance(data['now'], datetime)
        assert data['now'] == doc.now

    def test_now_save(self, db):
        epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

        doc = self.Doc()
        doc.now = epoch
        db.insert_one(doc)

        doc = db.get_queryset(self.Doc).find_one(doc.id)
        data = db.db.testdocs.find_one()

        assert isinstance(data['now'], datetime)
        assert data['now'] == epoch

    def test_now_load(self, db):
        epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)
        _id = db.db.testdocs.insert_one({'now': epoch}).inserted_id

        doc = db.get_queryset(self.Doc).find_one(_id)

        assert isinstance(doc.now, datetime)
        assert doc.now == epoch


class TestTimedeltaField:
    class Doc(Document):
        __collection__ = 'testdocs'
        td = fields.TimedeltaField()
        tdd = fields.TimedeltaField(default=timedelta(seconds=13))

    def test_init(self):
        doc = self.Doc()

        assert not hasattr(doc, 'td')
        assert doc.tdd == timedelta(seconds=13)

    def test_save(self, db):
        doc = self.Doc()

        doc.td = timedelta(seconds=26)
        db.save(doc)

        raw = db.db['testdocs'].find_one()

        assert raw['td'] == 26
        assert raw['tdd'] == 13

    def test_load(self, db):
        db.db['testdocs'].insert_one({'td': 26})

        doc = db(self.Doc).find_one()

        assert doc.td == timedelta(seconds=26)
        assert not hasattr(doc, 'tdd')

    def test_set__type_error(self):
        doc = self.Doc()

        with pytest.raises(TypeError):
            doc.td = 'string is wrong type'

    def test_faker(self):
        doc = create_fake(self.Doc)

        assert isinstance(doc.td, timedelta)
        assert isinstance(doc.tdd, timedelta)
