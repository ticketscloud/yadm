"""
Base classes for build database fields.
"""

from yadm.markers import AttributeNotSet, NoDefault, NotLoaded


class FieldDescriptor(object):
    """ Base desctiptor for fields
    """
    def __init__(self, name, field):
        self.name = name
        self.field = field

    def __repr__(self):
        class_name = type(self).__name__
        document_class_name = type(self.field.document_class).__name__
        return '<{} "{}.{}">'.format(class_name, document_class_name, self.name)

    def __get__(self, instance, owner):
        if instance is None:
            return self.field

        else:
            value = instance.__data__.get(self.name, AttributeNotSet)

            if value is AttributeNotSet:
                raise AttributeError(self.name)

            elif value is NotLoaded:
                value = self.load_deferred(instance)

            value = self.field.from_mongo(instance, value)
            instance.__data__[self.name] = value

            from yadm.documents import DocumentItemMixin

            if isinstance(value, DocumentItemMixin):
                value.__name__ = self.field.name
                value.__parent__ = instance

            return value

    def __set__(self, instance, value):
        if not isinstance(instance, type):
            value = self.field.prepare_value(instance, value)

            if value != instance.__data__.get(self.name):
                instance.__fields_changed__.add(self.name)

            instance.__data__[self.name] = value

        else:
            setattr(instance, self.name, value)

    def __delete__(self, instance):
        if not isinstance(instance, type):
            setattr(instance, self.name, AttributeNotSet)

    def load_deferred(self, instance):
        root = getattr(instance, '__document__', None)

        if root is None:
            qs = instance.__db__.get_queryset(instance.__class__)
            qs = qs.fields(self.name)
            doc = qs.with_id(instance.id)
            value = doc.__data__[self.name]
            value = value if value is not NotLoaded else None
            instance.__data__[self.name] = value

        else:
            qs = instance.__db__.get_queryset(root.__class__)
            proj = [i for i in instance.__path_names__ if isinstance(i, str)]
            qs = qs.fields('.'.join(proj + [self.name]))
            doc = qs.with_id(root.id)
            value = instance.__get_value__(doc).__data__[self.name]
            value = value if value is not NotLoaded else None
            instance.__data__[self.name] = value

        return value


class Field(object):
    """ Base field for all batabase fields

    .. py:attribute:: descriptor_class

        Class of desctiptor for work with field

    .. py:attribute:: default (NoDefault)

        Default value. It is not processed by ``prepare_value``
        in the creating document
    """

    descriptor_class = FieldDescriptor
    default = NoDefault
    name = None
    document_class = None

    def __init__(self, default=NoDefault):
        if default is not NoDefault:
            self.default = self.prepare_value(None, default)

    def __repr__(self):
        class_name = type(self).__name__
        document_class_name = type(self.document_class).__name__
        return '<{} "{}.{}">'.format(class_name, document_class_name, self.name)

    def contribute_to_class(self, document_class, name):
        """ Add field for document_class

        :param MetaDocument document_class: document class for add
        """
        self.name = name
        self.document_class = document_class
        self.document_class.__fields__[name] = self
        setattr(document_class, name, self.descriptor_class(name, self))

    def copy(self):
        """ Return copy of field
        """
        return self.__class__(default=self.default)

    def prepare_value(self, document, value):
        """ The method is called when value is assigned for the attribute

        :param BaseDocument document: document
        :param value: raw value
        :return: prepared value

        It must be accept one argument and return processed (e.g. casted) analog.
        Also it is called for the default value in the creating (not instance!)
        document.
        """
        return value

    def to_mongo(self, document, value):
        return value

    def from_mongo(self, document, value):
        return value
