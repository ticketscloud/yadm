from unittest import SkipTest

from yadm import fields
from yadm.documents import Document
from yadm.bulk import Bulk

from .test_database import BaseDatabaseTest


class BulkTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'

            i = fields.IntegerField()

        self.TestDoc = TestDoc

        if self.db.db.connection.server_info()['version'] < '2.6':
            self.__class__.skip = True
            raise SkipTest('Bulk not work with MongoDB < 2.6')

    def test_create(self):
        bulk = self.db.bulk(self.TestDoc)
        self.assertIsInstance(bulk, Bulk)

    def test_insert_one(self):
        doc = self.TestDoc()
        doc.i = 1

        bulk = self.db.bulk(self.TestDoc)
        bulk.insert(doc)

        self.assertEqual(self.db.db.testdocs.count(), 0)
        bulk.execute()
        self.assertEqual(self.db.db.testdocs.count(), 1)
        self.assertEqual(self.db.db.testdocs.find_one()['i'], 1)

    def test_insert_many(self):
        bulk = self.db.bulk(self.TestDoc)

        for i in range(10):
            doc = self.TestDoc()
            doc.i = i
            bulk.insert(doc)

        self.assertEqual(self.db.db.testdocs.count(), 0)
        bulk.execute()
        self.assertEqual(self.db.db.testdocs.count(), 10)

    def test_context_manager(self):
        with self.db.bulk(self.TestDoc) as bulk:
            doc = self.TestDoc()
            doc.i = 1
            bulk.insert(doc)

        self.assertEqual(self.db.db.testdocs.count(), 1)

    def test_context_manager_error(self):
        with self.assertRaises(RuntimeError):
            with self.db.bulk(self.TestDoc) as bulk:
                doc = self.TestDoc()
                doc.i = 1
                bulk.insert(doc)

                raise RuntimeError

        self.assertEqual(self.db.db.testdocs.count(), 0)
