from unittest import TestCase

from bson import ObjectId

from yadm.documents import Document


class ObjectIdFieldTest(TestCase):
    def setUp(self):
        class TestDoc(Document):
            pass

        self.TestDoc = TestDoc

    def test_default(self):
        self.assertRaises(AttributeError, getattr, self.TestDoc(), '_id')

    def test_setattr_objectid(self):
        _id = ObjectId()
        td = self.TestDoc()
        td._id = _id

        self.assertIsInstance(td._id, ObjectId)
        self.assertEqual(td._id, _id)

    def test_setattr_str(self):
        _id = ObjectId()
        td = self.TestDoc()
        td._id = str(_id)

        self.assertIsInstance(td._id, ObjectId)
        self.assertEqual(td._id, _id)
