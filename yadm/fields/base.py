"""
Base classes for build database fields.
"""

import structures


class DatabaseFieldDescriptor(structures.descriptors.FieldDescriptor):
    """ Descriptor for use with database fields
    """
    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)

        if instance is not None and hasattr(self.field, 'from_mongo'):
            return self.field.from_mongo(instance, value)
        else:
            return value


class DatabaseFieldMixin:
    """ Mixin for mix with stricture's Field

    .. py:attribute:: descriptor_class

        Class of desctiptor for work with field
    """
    descriptor_class = DatabaseFieldDescriptor

    def contribute_to_structure(self, structure, name):
        super().contribute_to_structure(structure, name)
        setattr(structure, name, self.descriptor_class(name, self))
