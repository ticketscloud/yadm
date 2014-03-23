from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class SimpleSetFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdoc'
            s = fields.SetField(fields.IntegerField)

        self.TestDoc = TestDoc

    def test_default(self):
        td = self.TestDoc()
        self.assertIsInstance(td.s, fields.set.Set)
        self.assertFalse(td.s)
        self.assertEqual(len(td.s), 0)
        self.assertEqual(td.s._data, set())

    def test_get(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)

        self.assertTrue(td.s)
        self.assertEqual(len(td.s), 3)
        self.assertEqual(td.s._data, {1, 2, 3})
        self.assertEqual(set(td.s), {1, 2, 3})
        self.assertRaises(TypeError, td.s.__getitem__, 1)

    def test_add(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.s.add(4)

        self.assertEqual(td.s, {1, 2, 3, 4})

    def test_add_typeerror(self):
        td = self.TestDoc()
        self.assertRaises(ValueError, td.s.add, 'not a number')

    def test_add_save(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.s.add(4)
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['s'], [1, 2, 3, 4])

    def test_remove(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.s.remove(2)

        self.assertEqual(td.s, {1, 3})

    def test_remove_save(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.s.remove(2)
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['s'], [1, 3])

    def test_add_to_set(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.s.add_to_set(4)

        self.assertEqual(td.s, {1, 2, 3, 4})

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['s'], [1, 2, 3, 4])

    def test_add_to_set_typeerror(self):
        td = self.TestDoc()
        self.assertRaises(ValueError, td.s.add_to_set, 'not a number')

    def test_pull(self):
        _id = self.db.db.testdoc.insert({'s': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.s.pull(2)

        self.assertEqual(td.s, {1, 3})

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['s'], [1, 3])
