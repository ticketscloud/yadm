from unittest import TestCase

from yadm.documents import Document
from yadm import fields


class DocumentsTest(TestCase):
    def setUp(self):
        class TestDoc(Document):
            i = fields.IntegerField
            b = fields.BooleanField

        self.TestDoc = TestDoc

    def test__db(self):
        td = self.TestDoc()
        self.assertIs(td.__db__, None)


    def test_fields(self):
        self.assertEqual(set(self.TestDoc.__fields__), {'_id', 'i', 'b'})

    def test_inheritance_fields(self):
        class InhTestDoc(self.TestDoc):
            s = fields.IntegerField

        self.assertEqual(set(InhTestDoc.__fields__), {'_id', 'i', 'b', 's'})
        self.assertEqual(set(self.TestDoc.__fields__), {'_id', 'i', 'b'})


    def test_fields_changed(self):
        td = self.TestDoc()

        self.assertNotIn('i', td.__fields_changed__)
        self.assertNotIn('b', td.__fields_changed__)

        td.i = 13
        self.assertIn('i', td.__fields_changed__)
        self.assertNotIn('b', td.__fields_changed__)

        td.b = True
        self.assertIn('i', td.__fields_changed__)
        self.assertIn('b', td.__fields_changed__)
