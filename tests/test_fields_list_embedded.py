from yadm import fields
from yadm.documents import Document, EmbeddedDocument

from .test_database import BaseDatabaseTest


class ListEmbeddedFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestEDoc(EmbeddedDocument):
            i = fields.IntegerField()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            li = fields.ListField(fields.EmbeddedDocumentField(TestEDoc))

        self.TestEDoc = TestEDoc
        self.TestDoc = TestDoc

    def test_save(self):
        td = self.TestDoc()

        ted = self.TestEDoc()
        ted.i = 13
        td.li.append(ted)

        ted = self.TestEDoc()
        ted.i = 42
        td.li.append(ted)

        self.db.insert(td)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 13)
        self.assertEqual(data['li'][1]['i'], 42)

    def test_load(self):
        self.db.db.testdocs.insert({'li': [{'i': 13}, {'i': 42}]})

        doc = self.db.get_queryset(self.TestDoc).find_one()

        self.assertTrue(hasattr(doc, 'li'))
        self.assertEqual(len(doc.li), 2)

        for item in doc.li:
            self.assertIsInstance(item, self.TestEDoc)

        self.assertEqual(doc.li[0].i, 13)
        self.assertEqual(doc.li[1].i, 42)

    def test_push(self):
        td = self.TestDoc()
        self.db.insert(td)

        ted = self.TestEDoc()
        ted.i = 13
        td.li.push(ted)

        ted = self.TestEDoc()
        ted.i = 42
        td.li.push(ted)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 13)
        self.assertEqual(data['li'][1]['i'], 42)


class DeeperListEmbeddedFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestEEDoc(EmbeddedDocument):
            i = fields.IntegerField()

        class TestEDoc(EmbeddedDocument):
            lie = fields.ListField(fields.EmbeddedDocumentField(TestEEDoc))

        class TestDoc(Document):
            __collection__ = 'testdocs'
            li = fields.ListField(fields.EmbeddedDocumentField(TestEDoc))

        self.TestEEDoc = TestEEDoc
        self.TestEDoc = TestEDoc
        self.TestDoc = TestDoc

    def test_save(self):
        td = self.TestDoc()
        td.li.append(self.TestEDoc())

        teed = self.TestEEDoc()
        teed.i = 13
        td.li[0].lie.append(teed)

        self.db.save(td)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]), 1)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 13)

    def test_load(self):
        self.db.db.testdocs.insert({'li': [{'lie': [{'i': 13}]}]})

        doc = self.db.get_queryset(self.TestDoc).find_one()

        self.assertTrue(hasattr(doc, 'li'))
        self.assertEqual(len(doc.li), 1)
        self.assertTrue(hasattr(doc.li[0], 'lie'))
        self.assertEqual(len(doc.li[0].lie), 1)
        self.assertTrue(hasattr(doc.li[0].lie[0], 'i'))
        self.assertEqual(doc.li[0].lie[0].i, 13)

    def test_push(self):
        td = self.TestDoc()
        self.db.insert(td)

        td.li.push(self.TestEDoc())

        teed = self.TestEEDoc()
        teed.i = 13
        td.li[0].lie.push(teed)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 1)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 13)

    def test_pull(self):
        _id = self.db.db.testdocs.insert({'li': [{'lie': [{'i': 13}, {'i': 42}]}]})
        doc = self.db.get_queryset(self.TestDoc).find_one({'_id': _id})

        doc.li[0].lie.pull({'i': 42})

        data = self.db.db.testdocs.find_one({'_id': _id})

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 1)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 13)
