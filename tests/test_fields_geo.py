from yadm import fields
from yadm.documents import Document


def test_point_init():
    point = fields.Point(1, 2)
    assert point.longitude == 1
    assert point.latitude == 2


def test_point_get():
    longitude, latitude = fields.Point(1, 2)
    assert longitude == 1
    assert latitude == 2


def test_point_to_mongo():
    point = fields.Point(1, 2)
    assert point.to_mongo() == {'type': 'Point', 'coordinates': [1, 2]}


def test_point_from_mongo():
    point = fields.Point.from_mongo({'type': 'Point', 'coordinates': [1, 2]})
    assert isinstance(point, fields.Point)
    assert point.longitude == 1
    assert point.latitude == 2


def test_multipoint_init():
    mpoint = fields.MultiPoint([fields.Point(1, 2), fields.Point(3, 4)])

    assert len(mpoint._points) == 2
    assert len(mpoint) == 2
    assert isinstance(mpoint[0], fields.Point)

    assert mpoint[0].longitude == 1
    assert mpoint[0].latitude == 2
    assert mpoint[1].longitude == 3
    assert mpoint[1].latitude == 4


def test_multipoint_to_mongo():
    mpoint = fields.MultiPoint([fields.Point(1, 2), fields.Point(3, 4)])
    assert mpoint.to_mongo() == \
        {'type': 'MultiPoint', 'coordinates': [[1, 2], [3, 4]]}


def test_multipoint_from_mongo():
    mpoint = fields.MultiPoint.from_mongo(
        {'type': 'MultiPoint', 'coordinates': [[1, 2], [3, 4]]}
    )
    assert isinstance(mpoint, fields.MultiPoint)
    assert len(mpoint) == 2
    assert isinstance(mpoint[0], fields.Point)

    assert mpoint[0].longitude == 1
    assert mpoint[0].latitude == 2
    assert mpoint[1].longitude == 3
    assert mpoint[1].latitude == 4


class Doc(Document):
    __collection__ = 'testdoc'
    point = fields.PointField()


def test_doc_insert(db):
    doc = Doc()
    doc.point = fields.Point(1, 2)
    db.insert(doc)

    raw = db.db.testdoc.find_one({'_id': doc.id})
    assert 'point' in raw
    assert raw['point'] == {'type': 'Point', 'coordinates': [1, 2]}


def test_doc_get(db):
    _id = db.db.testdoc.insert(
        {'point': {'type': 'Point', 'coordinates': [1, 2]}}
    )
    doc = db(Doc).find_one(_id)

    assert hasattr(doc, 'point')
    assert isinstance(doc.point, fields.Point)
    assert doc.point.longitude == 1
    assert doc.point.latitude == 2


def test_doc_set_object():
    doc = Doc()
    doc.point = fields.Point(1, 2)
    assert isinstance(doc.point, fields.Point)
    assert doc.point.longitude == 1
    assert doc.point.latitude == 2


def test_doc_set_geojson():
    doc = Doc()
    doc.point = {'type': 'Point', 'coordinates': [1, 2]}
    assert isinstance(doc.point, fields.Point)
    assert doc.point.longitude == 1
    assert doc.point.latitude == 2
