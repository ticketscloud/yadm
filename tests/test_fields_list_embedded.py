from unittest import SkipTest

from yadm import fields
from yadm.documents import Document, EmbeddedDocument

from .test_database import BaseDatabaseTest


class ListEmbeddedFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class EDoc(EmbeddedDocument):
            i = fields.IntegerField()
            s = fields.StringField()

        class Doc(Document):
            __collection__ = 'docs'
            li = fields.ListField(fields.EmbeddedDocumentField(EDoc))

        self.EDoc = EDoc
        self.Doc = Doc

    def test_save(self):
        td = self.Doc()

        ted = self.EDoc()
        ted.i = 13
        td.li.append(ted)

        ted = self.EDoc()
        ted.i = 42
        td.li.append(ted)

        self.db.insert(td)

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 13)
        self.assertEqual(data['li'][1]['i'], 42)

    def test_load(self):
        self.db.db.docs.insert({'li': [{'i': 13}, {'i': 42}]})

        doc = self.db.get_queryset(self.Doc).find_one()

        self.assertTrue(hasattr(doc, 'li'))
        self.assertEqual(len(doc.li), 2)

        for item in doc.li:
            self.assertIsInstance(item, self.EDoc)

        self.assertEqual(doc.li[0].i, 13)
        self.assertEqual(doc.li[1].i, 42)

    def test_push(self):
        td = self.Doc()
        self.db.insert(td)

        ted = self.EDoc()
        ted.i = 13
        td.li.push(ted)

        ted = self.EDoc()
        ted.i = 42
        td.li.push(ted)

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 13)
        self.assertEqual(data['li'][1]['i'], 42)

    def test_replace(self):
        td = self.Doc()
        td.li.append(self.EDoc(i=13, s='13'))
        td.li.append(self.EDoc(i=42, s='42'))
        self.db.insert(td)

        td.li.replace({'i': 13}, self.EDoc(i=26))

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 26)
        self.assertNotIn('s', data['li'][0])
        self.assertEqual(data['li'][1]['i'], 42)
        self.assertEqual(data['li'][1]['s'], '42')

    def test_update(self):
        td = self.Doc()
        td.li.append(self.EDoc(i=13, s='13'))
        td.li.append(self.EDoc(i=42, s='42'))
        self.db.insert(td)

        td.li.update({'i': 13}, {'i': 26})

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIn('i', data['li'][0])
        self.assertEqual(data['li'][0]['i'], 26)
        self.assertEqual(data['li'][0]['s'], '13')
        self.assertEqual(data['li'][1]['i'], 42)
        self.assertEqual(data['li'][1]['s'], '42')


class DeeperListEmbeddedFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class EEDoc(EmbeddedDocument):
            i = fields.IntegerField()
            s = fields.StringField()

        class EDoc(EmbeddedDocument):
            lie = fields.ListField(fields.EmbeddedDocumentField(EEDoc))

        class Doc(Document):
            __collection__ = 'docs'
            li = fields.ListField(fields.EmbeddedDocumentField(EDoc))

        self.EEDoc = EEDoc
        self.EDoc = EDoc
        self.Doc = Doc

    def test_save(self):
        td = self.Doc()
        td.li.append(self.EDoc())

        teed = self.EEDoc()
        teed.i = 13
        td.li[0].lie.append(teed)

        self.db.save(td)

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 1)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 13)

    def test_load(self):
        self.db.db.docs.insert({'li': [{'lie': [{'i': 13}]}]})

        doc = self.db.get_queryset(self.Doc).find_one()

        self.assertTrue(hasattr(doc, 'li'))
        self.assertEqual(len(doc.li), 1)
        self.assertTrue(hasattr(doc.li[0], 'lie'))
        self.assertEqual(len(doc.li[0].lie), 1)
        self.assertTrue(hasattr(doc.li[0].lie[0], 'i'))
        self.assertEqual(doc.li[0].lie[0].i, 13)

    def test_push(self):
        td = self.Doc()
        self.db.insert(td)

        td.li.push(self.EDoc())

        teed = self.EEDoc()
        teed.i = 13
        td.li[0].lie.push(teed)

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 1)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 13)

    def test_pull(self):
        _id = self.db.db.docs.insert({'li': [{'lie': [{'i': 13}, {'i': 42}]}]})
        doc = self.db.get_queryset(self.Doc).find_one({'_id': _id})

        doc.li[0].lie.pull({'i': 42})

        data = self.db.db.docs.find_one({'_id': _id})

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 1)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 13)

    def test_replace(self):
        if not self.db.db.connection.server_info()['version'].startswith('2.4'):
            raise SkipTest('Work only with MongoDB 2.4')

        td = self.Doc()
        td.li.append(self.EDoc())
        td.li[0].lie.append(self.EEDoc(i=13, s='13'))
        td.li[0].lie.append(self.EEDoc(i=42, s='42'))
        self.db.insert(td)

        td.li[0].lie.replace({'i': 13}, self.EEDoc(i=26))

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 2)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 26)
        self.assertNotIn('s', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][1]['i'], 42)
        self.assertEqual(data['li'][0]['lie'][1]['s'], '42')

    def test_update(self):
        if not self.db.db.connection.server_info()['version'].startswith('2.4'):
            raise SkipTest('Work only with MongoDB 2.4')

        td = self.Doc()
        td.li.append(self.EDoc())
        td.li[0].lie.append(self.EEDoc(i=13, s='13'))
        td.li[0].lie.append(self.EEDoc(i=42, s='42'))
        self.db.insert(td)

        td.li[0].lie.update({'i': 13}, {'i': 26})

        data = self.db.db.docs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 1)
        self.assertIn('lie', data['li'][0])
        self.assertEqual(len(data['li'][0]['lie']), 2)
        self.assertIn('i', data['li'][0]['lie'][0])
        self.assertEqual(data['li'][0]['lie'][0]['i'], 26)
        self.assertEqual(data['li'][0]['lie'][0]['s'], '13')
        self.assertEqual(data['li'][0]['lie'][1]['i'], 42)
        self.assertEqual(data['li'][0]['lie'][1]['s'], '42')
