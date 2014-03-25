"""
Fields for basic data types.
"""

from bson import ObjectId
import structures

from yadm.fields.base import DatabaseFieldMixin


class ObjectIdField(DatabaseFieldMixin, structures.SimpleField):
    """ Field for ObjectId

    :param bool default_gen: generate default value if not set
    """
    func = ObjectId

    def __init__(self, default_gen=False):
        super().__init__()
        self.default_gen = default_gen

    @property
    def default(self):
        if self.default_gen:
            return ObjectId()
        else:
            return structures.markers.NoDefault


class BooleanField(DatabaseFieldMixin, structures.Boolean):
    """ Field for boolean values
    """

class IntegerField(DatabaseFieldMixin, structures.Integer):
    """ Field for integer
    """

class FloatField(DatabaseFieldMixin, structures.Float):
    """ Field for float
    """

class StringField(DatabaseFieldMixin, structures.String):
    """ Field for string
    """
