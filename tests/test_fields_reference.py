from bson import ObjectId

from yadm.documents import Document
from yadm import fields

from .test_database import BaseDatabaseTest


class ReferenceTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDocRef(Document):
            __collection__ = 'testdocs_ref'

        class TestDoc(Document):
            __collection__ = 'testdocs'
            ref = fields.ReferenceField(TestDocRef)

        self.TestDocRef = TestDocRef
        self.TestDoc = TestDoc

    def test_get(self):
        id_ref = self.db.db.testdocs_ref.insert({})
        id = self.db.db.testdocs.insert({'ref': id_ref})

        doc = self.db.get_queryset(self.TestDoc).with_id(id)

        self.assertEqual(doc._id, id)
        self.assertIsInstance(doc.ref, Document)
        self.assertEqual(doc.ref._id, id_ref)

    def test_set_objectid(self):
        id = self.db.db.testdocs.insert({})
        doc = self.db.get_queryset(self.TestDoc).with_id(id)

        doc.ref = self.db.db.testdocs_ref.insert({})
        self.db.save(doc)

        id_ref = self.db.db.testdocs.find_one({'_id': id})['ref']

        self.assertIsInstance(id_ref, ObjectId)
        self.assertIsInstance(doc.ref, Document)
        self.assertEqual(id_ref, doc.ref.id)

    def test_set_doc(self):
        id = self.db.db.testdocs.insert({})
        doc = self.db.get_queryset(self.TestDoc).with_id(id)

        doc_ref = self.TestDocRef()
        self.db.save(doc_ref)

        doc.ref = doc_ref
        self.db.save(doc)

        id_ref = self.db.db.testdocs.find_one({'_id': id})['ref']

        self.assertIsInstance(id_ref, ObjectId)
        self.assertIsInstance(doc.ref, Document)
        self.assertEqual(id_ref, doc.ref.id)
