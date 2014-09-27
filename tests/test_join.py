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

    def test_get_queryset(self):
        self.db.db.testdocs_ref.insert({'i': 3})
        qs = self.qs.join().get_queryset('ref')
        self.assertEqual(qs.count(), 2)
        self.assertEqual(
            {d.id for d in qs},
            {self.id_ref_1, self.id_ref_2},
        )


class ManyJoinTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        self.id_ref_1_1 = self.db.db.testdocs_ref_1.insert({'i': 11})
        self.id_ref_1_2 = self.db.db.testdocs_ref_1.insert({'i': 12})

        self.id_ref_2_1 = self.db.db.testdocs_ref_2.insert({'i': 21})
        self.id_ref_2_2 = self.db.db.testdocs_ref_2.insert({'i': 22})

        for n in range(10):
            self.db.db.testdocs.insert({
                'i': n,
                'ref_1': self.id_ref_1_1 if n % 2 else self.id_ref_1_2,
                'ref_2': self.id_ref_2_1 if n % 2 else self.id_ref_2_2,
            })

        class TestDocRef(Document):
            i = fields.IntegerField

        class TestDocRef_1(Document):
            __collection__ = 'testdocs_ref_1'

        class TestDocRef_2(Document):
            __collection__ = 'testdocs_ref_2'

        class TestDoc(Document):
            __collection__ = 'testdocs'
            i = fields.IntegerField
            ref_1 = fields.ReferenceField(TestDocRef_1)
            ref_2 = fields.ReferenceField(TestDocRef_2)

        self.TestDoc = TestDoc

        self.qs = self.db(self.TestDoc)
        self.join = self.qs.join('ref_1', 'ref_2')

    def test_join(self):
        for doc in self.join:
            for n in range(1, 3):
                fn = 'ref_{}'.format(n)
                self.assertIn(fn, doc.__data__)
                ref = doc.__data__[fn]
                self.assertIsInstance(ref, Document)
                self.assertEqual(
                    getattr(doc, fn).id,
                    getattr(self, ('id_{}_1'.format(fn) if doc.i % 2 else 'id_{}_2'.format(fn)))
                )
