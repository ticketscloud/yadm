from unittest import TestCase

from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class PointTest(TestCase):
    def test_init(self):
        point = fields.Point(1, 2)
        self.assertEqual(point.longitude, 1)
        self.assertEqual(point.latitude, 2)

    def test_get(self):
        longitude, latitude = fields.Point(1, 2)
        self.assertEqual(longitude, 1)
        self.assertEqual(latitude, 2)

    def test_to_mongo(self):
        point = fields.Point(1, 2)
        self.assertEqual(
            point.to_mongo(),
            {'type': 'Point', 'coordinates': [1, 2]},
        )

    def test_from_mongo(self):
        point = fields.Point.from_mongo({'type': 'Point', 'coordinates': [1, 2]})
        self.assertIsInstance(point, fields.Point)
        self.assertEqual(point.longitude, 1)
        self.assertEqual(point.latitude, 2)


class MultyPointTest(TestCase):
    def test_init(self):
        mpoint = fields.MultiPoint([fields.Point(1, 2), fields.Point(3, 4)])

        self.assertEqual(len(mpoint._points), 2)
        self.assertEqual(len(mpoint), 2)
        self.assertIsInstance(mpoint[0], fields.Point)

        self.assertEqual(mpoint[0].longitude, 1)
        self.assertEqual(mpoint[0].latitude, 2)
        self.assertEqual(mpoint[1].longitude, 3)
        self.assertEqual(mpoint[1].latitude, 4)

    def test_to_mongo(self):
        mpoint = fields.MultiPoint([fields.Point(1, 2), fields.Point(3, 4)])
        self.assertEqual(
            mpoint.to_mongo(),
            {'type': 'MultiPoint', 'coordinates': [[1, 2], [3, 4]]},
        )

    def test_from_mongo(self):
        mpoint = fields.MultiPoint.from_mongo(
            {'type': 'MultiPoint', 'coordinates': [[1, 2], [3, 4]]}
        )
        self.assertIsInstance(mpoint, fields.MultiPoint)
        self.assertEqual(len(mpoint), 2)
        self.assertIsInstance(mpoint[0], fields.Point)

        self.assertEqual(mpoint[0].longitude, 1)
        self.assertEqual(mpoint[0].latitude, 2)
        self.assertEqual(mpoint[1].longitude, 3)
        self.assertEqual(mpoint[1].latitude, 4)


class ComplexPointTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdoc'
            point = fields.PointField()

        self.TestDoc = TestDoc

    def test_insert(self):
        td = self.TestDoc()
        td.point = fields.Point(1, 2)
        self.db.insert(td)

        raw = self.db.db.testdoc.find_one({'_id': td.id})
        self.assertIn('point', raw)
        self.assertEqual(raw['point'], {'type': 'Point', 'coordinates': [1, 2]})

    def test_get(self):
        _id = self.db.db.testdoc.insert(
            {'point': {'type': 'Point', 'coordinates': [1, 2]}}
        )
        td = self.db(self.TestDoc).with_id(_id)

        self.assertTrue(hasattr(td, 'point'))
        self.assertIsInstance(td.point, fields.Point)
        self.assertEqual(td.point.longitude, 1)
        self.assertEqual(td.point.latitude, 2)
