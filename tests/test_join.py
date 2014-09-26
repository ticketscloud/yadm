from yadm.documents import Document
from yadm import fields

from .test_database import BaseDatabaseTest


class JoinTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        self.id_ref_1 = self.db.db.testdocs_ref.insert({'i': 1})
        self.id_ref_2 = self.db.db.testdocs_ref.insert({'i': 2})

        for n in range(10):
            self.db.db.testdocs.insert({
                'i': n,
                'ref': self.id_ref_1 if n % 2 else self.id_ref_2,
            })

        class TestDocRef(Document):
            __collection__ = 'testdocs_ref'
            i = fields.IntegerField

        class TestDoc(Document):
            __collection__ = 'testdocs'
            i = fields.IntegerField
            ref = fields.ReferenceField(TestDocRef)

        self.TestDocRef = TestDocRef
        self.TestDoc = TestDoc

        self.qs = self.db(self.TestDoc)
        self.join = self.qs.join('ref')

    def test_len(self):
        self.assertEqual(len(self.join), 10)

    def test_iter(self):
        self.assertEqual(len(list(self.join)), 10)

    def test_join(self):
        for doc in self.join:
            self.assertIn('ref', doc.__data__)
            ref = doc.__data__['ref']
            self.assertIsInstance(ref, Document)
            self.assertEqual(doc.ref.id, self.id_ref_1 if doc.i % 2 else self.id_ref_2)
