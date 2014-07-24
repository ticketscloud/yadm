from unittest import TestCase

from bson import ObjectId

from yadm.documents import Document
from yadm.fields.simple import StringField, IntegerField


class ChoicesTest(TestCase):
    def setUp(self):
        class TestDoc(Document):
            string = StringField(choices={'qwerty', 'zzz', 'asd'})

        self.TestDoc = TestDoc

    def test_set_valid(self):
        doc = self.TestDoc()
        doc.string = 'zzz'
        self.assertEqual(doc.string, 'zzz')

    def test_set_invalid(self):
        doc = self.TestDoc()
        self.assertRaises(ValueError, setattr, doc, 'string', 'invalid')


class ChoicesWithDefaultTest(TestCase):
    def test_valid(self):
        class TestDoc(Document):
            string = IntegerField(default=0, choices={0, 13, 42})

        doc = TestDoc()
        self.assertEqual(doc.string, 0)

    def test_invalid(self):
        with self.assertRaises(ValueError):
            IntegerField(default=1, choices={0, 13, 42})


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
