from bson import ObjectId
import structures


class DatabaseFieldDescriptor(structures.descriptors.FieldDescriptor):
    pass


class DatabaseFieldMixin:
    def contribute_to_structure(self, structure, name):
        super().contribute_to_structure(structure, name)
        setattr(structure, name, DatabaseFieldDescriptor(name, self))


class ObjectIdField(DatabaseFieldMixin, structures.SimpleField):
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
    pass


class IntegerField(DatabaseFieldMixin, structures.Integer):
    pass


class FloatField(DatabaseFieldMixin, structures.Float):
    pass


class StringField(DatabaseFieldMixin, structures.String):
    pass
