from unittest import TestCase, SkipTest

import pymongo

from yadm.database import Database
from yadm.documents import Document
from yadm.queryset import QuerySet
from yadm.serialize import from_mongo
from yadm import fields


class BaseDatabaseTest(TestCase):
    skip = False

    def setUp(self):
        if self.skip:
            raise SkipTest

        try:
            self.client = pymongo.MongoClient("localhost", 27017, tz_aware=True)
            self.client.drop_database('test')
        except pymongo.errors.ConnectionFailure:
            self.__class__.skip = True
            raise SkipTest("Can't connect to database (localhost:27017/test)")

        self.db = Database(self.client, 'test')


class DatabaseTest(BaseDatabaseTest):
    def test_init(self):
        self.assertIsInstance(self.db.client, pymongo.mongo_client.MongoClient)
        self.assertIsInstance(self.db.db, pymongo.database.Database)
        self.assertEqual(self.db.db.name, 'test')
        self.assertEqual(self.db.name, 'test')

    def test_get_collection(self):
        class TestDoc(Document):
            __collection__ = 'testdocs'

        collection = self.db._get_collection(TestDoc)

        self.assertIsInstance(collection, pymongo.collection.Collection)
        self.assertEqual(collection.name, 'testdocs')

    def test_get_queryset(self):
        class TestDoc(Document):
            __collection__ = 'testdocs'

        queryset = self.db.get_queryset(TestDoc)

        self.assertIsInstance(queryset, QuerySet)
        self.assertIs(queryset._db, self.db)
        self.assertIs(queryset._document_class, TestDoc)


class DatabaseSaveTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            i = fields.IntegerField

        self.TestDoc = TestDoc

    def test_insert(self):
        td = self.TestDoc()
        td.i = 13

        self.db.insert(td)

        self.assertEqual(self.db.db.testdocs.find().count(), 1)
        self.assertEqual(self.db.db.testdocs.find()[0]['i'], 13)
        self.assertFalse(td.__fields_changed__)
        self.assertIs(td.__db__, self.db)

    def test_save_new(self):
        td = self.TestDoc()
        td.i = 13

        self.db.save(td)

        self.assertEqual(self.db.db.testdocs.find().count(), 1)
        self.assertEqual(self.db.db.testdocs.find()[0]['i'], 13)
        self.assertFalse(td.__fields_changed__)
        self.assertIs(td.__db__, self.db)

    def test_save(self):
        col = self.db.db.testdocs
        col.insert({'i': 13})

        td = from_mongo(self.TestDoc, col.find_one({'i': 13}))
        td.i = 26

        self.db.save(td)

        self.assertEqual(self.db.db.testdocs.find().count(), 1)
        self.assertEqual(self.db.db.testdocs.find()[0]['i'], 26)
        self.assertFalse(td.__fields_changed__)
        self.assertIs(td.__db__, self.db)


class DatabaseRemoveTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            i = fields.IntegerField

        self.TestDoc = TestDoc

    def test_remove(self):
        col = self.db.db.testdocs
        col.insert({'i': 13})

        td = from_mongo(self.TestDoc, col.find_one({'i': 13}))

        self.assertEqual(col.count(), 1)
        self.db.remove(td)
        self.assertEqual(col.count(), 0)


class DatabaseReloadDocumentTest(BaseDatabaseTest):
    def test_reload(self):
        class TestDoc(Document):
            __collection__ = 'testdocs'
            i = fields.IntegerField()

        doc = TestDoc()
        doc.i = 1
        self.db.insert(doc)

        self.db.db.testdocs.update({'_id': doc.id}, {'i': 2})

        self.assertEqual(doc.i, 1)
        new = self.db.reload(doc)
        self.assertEqual(doc.i, 2)
        self.assertIs(doc, new)

    def test_reload_new_instance(self):
        class TestDoc(Document):
            __collection__ = 'testdocs'
            i = fields.IntegerField()

        doc = TestDoc()
        doc.i = 1
        self.db.insert(doc)

        self.db.db.testdocs.update({'_id': doc.id}, {'i': 2})

        self.assertEqual(doc.i, 1)
        new = self.db.reload(doc, new_instance=True)
        self.assertEqual(doc.i, 1)
        self.assertIsNot(doc, new)
        self.assertEqual(new.i, 2)
