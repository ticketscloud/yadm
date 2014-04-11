from yadm import fields
from yadm.documents import Document, EmbeddedDocument

from .test_database import BaseDatabaseTest


class SetEmbeddedFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestEDoc(EmbeddedDocument):
            i = fields.IntegerField()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            li = fields.SetField(fields.EmbeddedDocumentField(TestEDoc))

        self.TestEDoc = TestEDoc
        self.TestDoc = TestDoc

    def test_save(self):
        td = self.TestDoc()

        ted = self.TestEDoc()
        ted.i = 13
        td.li.add(ted)

        ted = self.TestEDoc()
        ted.i = 42
        td.li.add(ted)

        self.db.insert(td)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])

        res = set()

        for item in data['li']:
            self.assertIn('i', item)
            self.assertIsInstance(item['i'], int)
            res.add(item['i'])

        self.assertEquals(res, {13, 42})

    def test_load(self):
        _id = self.db.db.testdocs.insert({'li': []})
        self.db.db.testdocs.update({'_id': _id}, {'$addToSet': {'li': {'i': 13}}})
        self.db.db.testdocs.update({'_id': _id}, {'$addToSet': {'li': {'i': 42}}})

        doc = self.db.get_queryset(self.TestDoc).find_one()

        self.assertTrue(hasattr(doc, 'li'))
        self.assertEqual(len(doc.li), 2)

        res = set()

        for item in doc.li:
            self.assertIsInstance(item, self.TestEDoc)
            self.assertTrue(hasattr(item, 'i'))
            self.assertIsInstance(item.i, int)
            res.add(item.i)

        self.assertEquals(res, {13, 42})

    def test_add_to_set(self):
        td = self.TestDoc()
        self.db.insert(td)

        ted = self.TestEDoc()
        ted.i = 13
        td.li.add_to_set(ted)

        ted = self.TestEDoc()
        ted.i = 42
        td.li.add_to_set(ted)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 13)
        self.assertEqual(data['li'][1]['i'], 42)
