from bson import ObjectId

from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class ListReferenceFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestRDoc(Document):
            __collection__ = 'testrdocs'
            i = fields.IntegerField()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            li = fields.ListField(fields.ReferenceField(TestRDoc))

        self.TestRDoc = TestRDoc
        self.TestDoc = TestDoc

    def test_save(self):
        td = self.TestDoc()

        trd_one = self.TestRDoc()
        trd_one.i = 13
        self.db.insert(trd_one)
        td.li.append(trd_one)

        trd_two = self.TestRDoc()
        trd_two.i = 42
        self.db.insert(trd_two)
        td.li.append(trd_two)

        self.db.insert(td)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)
        self.assertIsInstance(data['li'][0], ObjectId)
        self.assertIsInstance(data['li'][1], ObjectId)
        self.assertEqual(data['li'][0], trd_one.id)
        self.assertEqual(data['li'][1], trd_two.id)

    def test_load(self):
        self.db.db.testdocs.insert(
            {'li': [
                self.db.db.testrdocs.insert({'i': 13}),
                self.db.db.testrdocs.insert({'i': 42}),
            ]}
        )

        doc = self.db.get_queryset(self.TestDoc).find_one()

        self.assertTrue(hasattr(doc, 'li'))
        self.assertEqual(len(doc.li), 2)

        for item in doc.li:
            self.assertIsInstance(item, self.TestRDoc)

        self.assertEqual(doc.li[0].i, 13)
        self.assertEqual(doc.li[1].i, 42)
