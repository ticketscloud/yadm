from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class ListFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdoc'
            li = fields.ListField(fields.IntegerField())

        self.TestDoc = TestDoc

    def test_default(self):
        td = self.TestDoc()
        self.assertIsInstance(td.li, fields.list.List)
        self.assertFalse(td.li)
        self.assertEqual(len(td.li), 0)
        self.assertEqual(td.li._data, [])
        self.assertEqual(td.li, [])

    def test_get(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)

        self.assertTrue(td.li)
        self.assertEqual(len(td.li), 3)
        self.assertEqual(td.li._data, [1, 2, 3])
        self.assertEqual(list(td.li), [1, 2, 3])
        self.assertEqual(td.li[1], 2)

    def test_append(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.li.append(4)

        self.assertEqual(td.li, [1, 2, 3, 4])

    def test_append_valueerror(self):
        td = self.TestDoc()
        self.assertRaises(ValueError, td.li.append, 'not a number')

    def test_append_save(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.li.append(4)
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['li'], [1, 2, 3, 4])

    def test_remove(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.li.remove(2)

        self.assertEqual(td.li, [1, 3])

    def test_remove_save(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.li.remove(2)
        self.db.save(td)

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['li'], [1, 3])

    def test_push(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.li.push(4)

        self.assertEqual(td.li, [1, 2, 3, 4])

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['li'], [1, 2, 3, 4])

    def test_push_valueerror(self):
        td = self.TestDoc()
        self.assertRaises(ValueError, td.li.push, 'not a number')

    def test_pull(self):
        _id = self.db.db.testdoc.insert({'li': [1, 2, 3]})
        td = self.db.get_queryset(self.TestDoc).with_id(_id)
        td.li.pull(2)

        self.assertEqual(td.li, [1, 3])

        data = self.db.db.testdoc.find_one({'_id': _id})
        self.assertEqual(data['li'], [1, 3])
