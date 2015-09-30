"""
Base classes for build database fields.
"""

from yadm.markers import AttributeNotSet, NotLoaded


class NotLoadedError(Exception):
    pass


class FieldDescriptor:
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
        name = self.name

        if instance is None:
            return self.field

        elif name in instance.__changed__:
            value = instance.__changed__[name]

        elif name in instance.__cache__:
            value = instance.__cache__[name]

        elif name in instance.__raw__:
            raw = instance.__raw__[name]
            value = self.field.from_mongo(instance, raw)

            from yadm.documents import DocumentItemMixin
            if isinstance(value, DocumentItemMixin):
                value.__name__ = self.field.name
                value.__parent__ = instance

            instance.__cache__[name] = value

        else:
            value = self.field.get_default(instance)
            instance.__changed__[name] = value

        if value is AttributeNotSet:
            raise AttributeError(name)

        return value

    def __set__(self, instance, value):
        if not isinstance(instance, type):
            value = self.field.prepare_value(instance, value)

            name = self.name

            if name in instance.__changed__:
                value_old = instance.__changed__[name]
            elif name in instance.__cache__:
                value_old = instance.__cache__[name]
            elif name in instance.__raw__:
                value_old = getattr(instance, name, AttributeNotSet)
            else:
                value_old = AttributeNotSet

            if value != value_old:
                instance.__changed__[name] = value
                self.field.set_parent_changed(instance)

        else:
            raise TypeError("can't set field directly")

    def __delete__(self, instance):
        if not isinstance(instance, type):
            setattr(instance, self.name, AttributeNotSet)


class Field:
    """ Base field for all batabase fields

    .. py:attribute:: descriptor_class

        Class of desctiptor for work with field
    """
    descriptor_class = FieldDescriptor
    name = None
    document_class = None

    def __init__(self):
        pass

    def __repr__(self):
        class_name = type(self).__name__
        doc_class_name = self.document_class and self.document_class.__name__
        return '<{} "{}.{}">'.format(class_name, doc_class_name, self.name)

    def contribute_to_class(self, document_class, name):
        """ Add field for document_class

        :param MetaDocument document_class: document class for add
        """
        self.name = name
        self.document_class = document_class
        self.document_class.__fields__[name] = self
        setattr(document_class, name, self.descriptor_class(name, self))

    def set_parent_changed(self, instance):
        from yadm.documents import DocumentItemMixin
        if (isinstance(instance, DocumentItemMixin) and
                instance.__parent__ is not None):

            first = list(instance.__path__)[-1]
            first_name = first.__name__
            instance.__document__.__changed__[first_name] = first
            # TODO: update __changed__ for full path

    def copy(self):
        """ Return copy of field
        """
        return self.__class__()

    def get_default(self, document):
        """ Return default value
        """
        return AttributeNotSet

    def get_fake(self, document, faker, deep):
        """ Return fake data for testing
        """
        return self.get_default(document)

    def prepare_value(self, document, value):
        """ The method is called when value is assigned
            for the attribute

        :param BaseDocument document: document
        :param value: raw value
        :return: prepared value

        It must be accept `value` argument and return
        processed (e.g. casted) analog. Also it is called
        once for the default value.
        """
        return value

    def to_mongo(self, document, value):
        return value

    def from_mongo(self, document, value):
        if value is AttributeNotSet:
            raise AttributeError(self.name)
        elif value is NotLoaded:
            raise NotLoadedError(self.name)

        return value


class DefaultMixin:
    default = AttributeNotSet

    def __init__(self, default=AttributeNotSet):
        self.default = default
        super().__init__()

    def get_default(self, document):
        return self.default

    def copy(self):
        """ Return copy of field
        """
        return self.__class__(default=self.default)
