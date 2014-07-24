from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class MapFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdoc'
            map = fields.MapField(fields.IntegerField())

        self.TestDoc = TestDoc

    def test_default(self):
        td = self.TestDoc()
        self.assertIsInstance(td.map, fields.map.Map)
        self.assertFalse(td.map)
        self.assertEqual(len(td.map), 0)
        self.assertEqual(td.map._data, {})
        self.assertEqual(td.map, {})

    def test_get(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)

        self.assertTrue(td.map)
        self.assertEqual(len(td.map), 3)
        self.assertEqual(td.map._data, {'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(dict(td.map), {'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(td.map, {'a': 1, 'b': 2, 'c': 3})
        self.assertEqual(td.map['b'], 2)

    def test_setattr(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.map['d'] = 4

        self.assertEqual(td.map, {'a': 1, 'b': 2, 'c': 3, 'd': 4})

    def test_setattr_valueerror(self):
        td = self.TestDoc()
        with self.assertRaises(ValueError):
            td.map['d'] = 'not a number'

    def test_setattr_keyerror(self):
        td = self.TestDoc()
        with self.assertRaises(KeyError):
            td.map['not exist']

    def test_setattr_save(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.map['d'] = 4
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['map'], {'a': 1, 'b': 2, 'c': 3, 'd': 4})
        self.assertEqual(td.map, {'a': 1, 'b': 2, 'c': 3, 'd': 4})

    def test_remove(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        del td.map['b']

        self.assertEqual(td.map, {'a': 1, 'c': 3})

    def test_remove_save(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        del td.map['b']
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['map'], {'a': 1, 'c': 3})

    def test_set(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.map.set('d', 4)

        self.assertEqual(td.map, {'a': 1, 'b': 2, 'c': 3, 'd': 4})

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['map'], {'a': 1, 'b': 2, 'c': 3, 'd': 4})

    def test_set_runtimeerror(self):
        td = self.TestDoc()
        with self.assertRaises(RuntimeError):
            td.map.set('key', 1)

    def test_set_valueerror(self):
        td = self.TestDoc()
        self.db.save(td)
        with self.assertRaises(ValueError):
            td.map.set('key', 'not a number')

    def test_unset(self):
        _id = self.db.db.testdoc.insert({'map': {'a': 1, 'b': 2, 'c': 3}})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.map.unset('b')

        self.assertEqual(td.map, {'a': 1, 'c': 3})

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['map'], {'a': 1, 'c': 3})
