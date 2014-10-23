from unittest import SkipTest

from pymongo.errors import BulkWriteError

from yadm import fields
from yadm.documents import Document
from yadm.bulk import Bulk

from .test_database import BaseDatabaseTest


class BulkTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        if self.db.db.connection.server_info()['version'] < '2.6':
            self.__class__.skip = True
            raise SkipTest('Bulk not work with MongoDB < 2.6')

        class TestDoc(Document):
            __collection__ = 'testdocs'

            i = fields.IntegerField()

        self.TestDoc = TestDoc

        self.db.db.testdocs.ensure_index([('i', 1)], unique=True)

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

        self.assertTrue(bulk.result)
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

    def test_insert_type_error(self):
        class Doc(Document):
            pass

        bulk = self.db.bulk(self.TestDoc)

        with self.assertRaises(TypeError):
            bulk.insert(Doc())

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

    def test_result(self):
        with self.db.bulk(self.TestDoc) as bulk:
            doc = self.TestDoc()
            doc.i = 1
            bulk.insert(doc)

        self.assertEqual(bulk.result.n_inserted, 1)

    def test_result_write_error(self):
        with self.db.bulk(self.TestDoc, raise_on_errors=False) as bulk:
            doc = self.TestDoc()
            doc.i = 1
            bulk.insert(doc)

            doc = self.TestDoc()
            doc.i = 2
            bulk.insert(doc)

            doc = self.TestDoc()
            doc.i = 1
            bulk.insert(doc)

        self.assertFalse(bulk.result)
        self.assertEqual(self.db.db.testdocs.count(), 2)
        self.assertEqual(bulk.result.n_inserted, 2)
        self.assertEqual(len(bulk.result.write_errors), 1)
        self.assertEqual(bulk.result.write_errors[0].document.i, 1)

    def test_result_write_error_raise(self):
        with self.assertRaises(BulkWriteError):
            with self.db.bulk(self.TestDoc) as bulk:
                doc = self.TestDoc()
                doc.i = 1
                bulk.insert(doc)

                doc = self.TestDoc()
                doc.i = 2
                bulk.insert(doc)

                doc = self.TestDoc()
                doc.i = 1
                bulk.insert(doc)

        self.assertEqual(bulk.result.n_inserted, 2)
        self.assertEqual(len(bulk.result.write_errors), 1)
        self.assertEqual(bulk.result.write_errors[0].document.i, 1)

    def test_result_write_error_ordered(self):
        with self.db.bulk(self.TestDoc, ordered=True, raise_on_errors=False) as bulk:
            for i in range(10):
                doc = self.TestDoc()
                doc.i = i
                bulk.insert(doc)

                doc = self.TestDoc()
                doc.i = 1
                bulk.insert(doc)

        self.assertEqual(self.db.db.testdocs.count(), 2)
        self.assertEqual(bulk.result.n_inserted, 2)
        self.assertEqual(len(bulk.result.write_errors), 1)
        self.assertEqual(bulk.result.write_errors[0].document.i, 1)
