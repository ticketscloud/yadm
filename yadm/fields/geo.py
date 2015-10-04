"""
Fields for geo data

See: http://docs.mongodb.org/manual/applications/geospatial-indexes/

GeoJSON: http://geojson.org/geojson-spec.html

"""
from collections.abc import Sequence

from yadm.documents import DocumentItemMixin
from yadm.fields.base import Field


TYPES = []


def _geo_type(type):
    # class decorator for add geo types to TYPES
    TYPES.append(type)
    return type


class Geo(DocumentItemMixin):
    """ Base class for GeoJSON data
    """
    type = None


class GeoCoordinates(Geo):
    """ Base class for GeoJSON data with coordinates
    """
    def get_coordinates(self):
        raise NotImplementedError('get_coordinates must be implemented')

    def to_mongo(self):
        return {
            'type': self.type,
            'coordinates': self.get_coordinates(),
        }


@_geo_type
class Point(GeoCoordinates):
    """ Class for GeoJSON Point objects

    See: http://geojson.org/geojson-spec.html#id2
    """
    type = 'Point'

    def __init__(self, longitude, latitude):
        self.longitude = longitude
        self.latitude = latitude

    def __getitem__(self, idx):
        return (self.longitude, self.latitude)[idx]

    def get_coordinates(self):
        return [self.longitude, self.latitude]

    @classmethod
    def from_mongo(cls, data):
        try:
            coordinates = data['coordinates']
        except KeyError:
            raise ValueError('coordinates not found in data: "{!r}"'.format(data))

        try:
            longitude, latitude = coordinates
        except IndexError:
            raise ValueError('wrong coordinates in data: "{!r}"'.format(data))

        return cls(longitude, latitude)


@_geo_type
class MultiPoint(GeoCoordinates, Sequence):
    """ Class for GeoJSON MultiPoint objects

    See: http://geojson.org/geojson-spec.html#id5
    """
    type = 'MultiPoint'

    def __init__(self, points):
        self._points = points

    def __len__(self):
        return len(self._points)

    def __iter__(self):
        return self._points

    def __getitem__(self, item):
        return self._points[item]

    def get_coordinates(self):
        return [p.to_mongo()['coordinates'] for p in self._points]

    @classmethod
    def from_mongo(cls, data):
        try:
            coordinates = data['coordinates']
        except KeyError:
            raise ValueError('coordinates not found in data: "{!r}"'.format(data))

        return cls([Point(*c) for c in coordinates])


class GeoField(Field):
    """ Base field for GeoJSON objects
    """
    def __init__(self, types=TYPES):
        self.types = types
        self.types_dict = {t.type: t for t in types}

    def to_mongo(self, document, geo):
        return geo.to_mongo() if geo is not None else None

    def from_mongo(self, document, data):
        if data is None:
            return None

        geo_type = self.types_dict.get(data['type'])

        if geo_type is None:
            raise ValueError('unknown type in data: "{!r}"'.format(data))

        return geo_type.from_mongo(data)


class GeoOneTypeField(GeoField):
    """ Base field for GeoJSON objects
        with one acceptable type
    """
    type = None

    def __init__(self):
        if self.type is None:
            raise NotImplementedError('attribute "type" must be implemented')

        self.types = [self.type]
        self.types_dict = {self.type.type: self.type}

    def prepare_value(self, document, value):
        if isinstance(value, (self.type, type(None))):
            return value
        else:
            raise TypeError(value)

    def _get_fake_point(self, faker):
        return Point(float(faker.longitude()), float(faker.latitude()))


class PointField(GeoOneTypeField):
    """ Field for Point
    """
    type = Point

    def get_fake(self, document, faker, depth):
        return self._get_fake_point(faker)


class MultiPointField(GeoOneTypeField):
    """ Field for MultiPoint
    """
    type = MultiPoint

    def get_fake(self, document, faker, depth):
        return [self._get_fake_point(faker) for _ in range(4)]
