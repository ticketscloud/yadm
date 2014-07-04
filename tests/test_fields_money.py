from unittest import TestCase

from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class MoneyTest(TestCase):
    def test_str(self):
        self.assertEqual(str(fields.Money('3.14')), '3.14')
        self.assertEqual(str(fields.Money('0.14')), '0.14')
        self.assertEqual(str(fields.Money('30.14')), '30.14')
        self.assertEqual(str(fields.Money('3.1')), '3.10')
        self.assertEqual(str(fields.Money('0.1')), '0.10')

    def test_to_mongo(self):
        self.assertEqual(fields.Money('3.14').to_mongo(), 314)
        self.assertEqual(fields.Money('0.14').to_mongo(), 14)
        self.assertEqual(fields.Money('30.14').to_mongo(), 3014)
        self.assertEqual(fields.Money('3.1').to_mongo(), 310)
        self.assertEqual(fields.Money('0.1').to_mongo(), 10)


class MoneyFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            money = fields.MoneyField()

        self.TestDoc = TestDoc

    def test_save(self):
        doc = self.TestDoc()
        doc.money = fields.Money('3.14')
        self.db.save(doc)

        data = self.db.db.testdocs.find_one()

        self.assertIn('money', data)
        self.assertIsInstance(data['money'], int)
        self.assertEqual(data['money'], 314)

    def test_load(self):
        self.db.db.testdocs.insert({'money': 314})

        doc = self.db.get_queryset(self.TestDoc).find_one()

        self.assertTrue(hasattr(doc, 'money'))
        self.assertIsInstance(doc.money, fields.Money)
        self.assertEqual(doc.money, fields.Money('3.14'))

    def test_load_and_save(self):
        self.db.db.testdocs.insert({'money': 314})

        doc = self.db.get_queryset(self.TestDoc).find_one()
        self.db.save(doc)

        self.assertTrue(hasattr(doc, 'money'))
        self.assertIsInstance(doc.money, fields.Money)
        self.assertEqual(doc.money, fields.Money('3.14'))

        data = self.db.db.testdocs.find_one()

        self.assertIn('money', data)
        self.assertIsInstance(data['money'], int)
        self.assertEqual(data['money'], 314)
