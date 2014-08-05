from bson import ObjectId

from yadm import fields
from yadm.documents import Document, EmbeddedDocument

from .test_database import BaseDatabaseTest


class EmbeddedDocumentFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class ETestDoc(EmbeddedDocument):
            i = fields.IntegerField()

        class TestDoc(Document):
            __collection__ = 'testdoc'
            e = fields.EmbeddedDocumentField(ETestDoc)

        self.ETestDoc = ETestDoc
        self.TestDoc = TestDoc

    def test_default(self):
        td = self.TestDoc()
        self.assertFalse(hasattr(td, 'e'))

    def test_get(self):
        _id = self.db.db.testdoc.insert({'e': {'i': 13}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)

        self.assertTrue(hasattr(td, 'e'))
        self.assertIsInstance(td.e, self.ETestDoc)
        self.assertTrue(hasattr(td.e, 'i'))
        self.assertIsInstance(td.e.i, int)
        self.assertEqual(td.e.i, 13)

    def test_set(self):
        td = self.TestDoc()
        td.e = self.ETestDoc()

        self.assertTrue(hasattr(td, 'e'))
        self.assertIsInstance(td.e, self.ETestDoc)
        self.assertFalse(hasattr(td.e, 'i'))

        td.e.i = 13
        self.assertTrue(hasattr(td.e, 'i'))
        self.assertIsInstance(td.e.i, int)
        self.assertEqual(td.e.i, 13)

    def test_set_typeerror(self):
        class FailETestDoc(EmbeddedDocument):
            s = fields.StringField

        td = self.TestDoc()
        self.assertRaises(TypeError, setattr, td, 'e', FailETestDoc())

    def test_set_insert(self):
        td = self.TestDoc()
        td.e = self.ETestDoc()
        td.e.i = 13
        self.db.insert(td)

        data = self.db.db.testdoc.find_one({'_id': td.id})
        self.assertEqual(data, {'_id': td.id, 'e': {'i': 13}})

    def test_set_save(self):
        _id = self.db.db.testdoc.insert({'e': {'i': 13}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)

        td.e.i = 26
        # td.__fields_changed__.add('e.i')
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data, {'_id': _id, 'e': {'i': 26}})


class EmbeddedDocumentWithIdFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class ETestDoc(EmbeddedDocument):
            id = fields.ObjectIdField(default_gen=True)
            i = fields.IntegerField()

        class TestDoc(Document):
            __collection__ = 'testdoc'
            e = fields.EmbeddedDocumentField(ETestDoc)

        self.ETestDoc = ETestDoc
        self.TestDoc = TestDoc

    def test_load_default(self):
        td = self.TestDoc({'e': {'i': 13}})
        self.assertTrue(hasattr(td, 'e'))
        self.assertTrue(hasattr(td.e, 'i'))
        self.assertTrue(hasattr(td.e, 'id'))
        self.assertEqual(td.e.i, 13)
        self.assertIsInstance(td.e.id, ObjectId)
