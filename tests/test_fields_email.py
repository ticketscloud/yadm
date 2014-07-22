from unittest import TestCase

from yadm.documents import Document
from yadm.fields.email import EmailField


class EmailFieldTest(TestCase):
    def test_ok(self):
        class TestDoc(Document):
            e = EmailField()

        td = TestDoc()
        td.e = 'E@mA.iL'

        self.assertEqual(td.e, 'e@ma.il')

    def test_error(self):
        class TestDoc(Document):
            e = EmailField()

        td = TestDoc()

        with self.assertRaises(ValueError):
            td.e = 'EmA.iL'
