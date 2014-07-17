from datetime import datetime, timezone

from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class DatetimeFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            dt = fields.DatetimeField()
            now = fields.DatetimeField(auto_now=True)

        self.TestDoc = TestDoc

    def test_cast(self):
        doc = self.TestDoc()
        doc.dt = datetime(1970, 1, 1, tzinfo=timezone.utc).isoformat()
        self.assertIsInstance(doc.dt, datetime)

    def test_default_none(self):
        doc = self.TestDoc()
        self.assertFalse(hasattr(doc, 'dt'))

    def test_save(self):
        doc = self.TestDoc()
        doc.dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
        self.db.insert(doc)

        doc = self.db.get_queryset(self.TestDoc).with_id(doc.id)
        data = self.db.db.testdocs.find_one()

        self.assertIsInstance(data['dt'], datetime)
        self.assertEqual(data['dt'], doc.dt)

    def test_load(self):
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        _id = self.db.db.testdocs.insert({'dt': epoch})

        doc = self.db.get_queryset(self.TestDoc).with_id(_id)

        self.assertIsInstance(doc.dt, datetime)
        self.assertEqual(doc.dt, epoch)

    def test_default_auto_now(self):
        doc = self.TestDoc()
        self.assertTrue(hasattr(doc, 'now'))
        self.assertIsInstance(doc.now, datetime)

    def test_default_now_save(self):
        doc = self.TestDoc()
        self.db.insert(doc)

        doc = self.db.get_queryset(self.TestDoc).with_id(doc.id)
        data = self.db.db.testdocs.find_one()

        self.assertIsInstance(data['now'], datetime)
        self.assertEqual(data['now'], doc.now)

    def test_now_save(self):
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

        doc = self.TestDoc()
        doc.now = epoch
        self.db.insert(doc)

        doc = self.db.get_queryset(self.TestDoc).with_id(doc.id)
        data = self.db.db.testdocs.find_one()

        self.assertIsInstance(data['now'], datetime)
        self.assertEqual(data['now'], epoch)

    def test_now_load(self):
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        _id = self.db.db.testdocs.insert({'now': epoch})

        doc = self.db.get_queryset(self.TestDoc).with_id(_id)

        self.assertIsInstance(doc.now, datetime)
        self.assertEqual(doc.now, epoch)
