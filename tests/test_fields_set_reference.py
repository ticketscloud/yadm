from bson import ObjectId

from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class SetReferenceFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestRDoc(Document):
            __collection__ = 'testrdocs'
            i = fields.IntegerField()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            li = fields.SetField(fields.ReferenceField(TestRDoc))

        self.TestRDoc = TestRDoc
        self.TestDoc = TestDoc

    def test_save(self):
        td = self.TestDoc()

        trd_one = self.TestRDoc()
        trd_one.i = 13
        self.db.insert(trd_one)
        td.li.add(trd_one)

        trd_two = self.TestRDoc()
        trd_two.i = 42
        self.db.insert(trd_two)
        td.li.add(trd_two)

        self.db.insert(td)

        data = self.db.db.testdocs.find_one()

        self.assertIn('li', data)
        self.assertEqual(len(data['li']), 2)

        res = set()

        for item in data['li']:
            self.assertIsInstance(item, ObjectId)
            res.add(item)

        self.assertEquals(res, {trd_one.id, trd_two.id})

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

        res = set()

        for item in doc.li:
            self.assertIsInstance(item, self.TestRDoc)
            self.assertTrue(hasattr(item, 'i'))
            self.assertIsInstance(item.i, int)
            res.add(item.i)

        self.assertEquals(res, {13, 42})
