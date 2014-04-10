from yadm import fields
from yadm.documents import Document
from yadm.markers import NotLoaded

from .test_database import BaseDatabaseTest


class DeferredLoadTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'

            i = fields.IntegerField()
            s = fields.StringField()

        self.TestDoc = TestDoc

    def test(self):
        _id = self.db.db.testdocs.insert({'i': 13, 's': 'test'})
        doc = self.db.get_queryset(self.TestDoc).fields('i').with_id(_id)

        self.assertEqual(doc.__data__, {'_id': _id, 'i': 13, 's': NotLoaded})

        self.assertTrue(hasattr(doc, 'i'))
        self.assertEqual(doc.i, 13)

        self.assertTrue(hasattr(doc, 's'))
        self.assertEqual(doc.s, 'test')
