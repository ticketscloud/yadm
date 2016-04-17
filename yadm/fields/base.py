"""
Base classes for build database fields.
"""
import functools

from yadm.markers import AttributeNotSet, NotLoaded


class NotLoadedError(Exception):
    """ Raise if value marked as not loaded.

    .. code:: python

        doc = db(Doc).fields('a').find_one()
        try:
            doc.b
        except NotLoadedError:
            print("raised!")
    """


def pass_null(method):
    @functools.wraps(method)
    def wrapper(self, document, value):
        if value is None:
            return None
        else:
            return method(self, document, value)

    return wrapper


class FieldDescriptor:
    """ Base desctiptor for fields.

    .. py:attribute:: name

    Name of field

    .. py:attribute:: field

    Field instance for this desctiptor

    """
    def __init__(self, name, field):
        self.name = name
        self.field = field

    def __repr__(self):  # pragma: no cover
        class_name = type(self).__name__
        document_class_name = type(self.field.document_class).__name__
        return '<{} "{}.{}">'.format(class_name, document_class_name, self.name)

    def __get__(self, instance, owner):
        """ Get python value from document.

        1. Lookup in __changed__;
        2. Lookup in __cache__;
        3. Lookup in __raw__:

            - if AttributeNotSet -- call Field.get_if_attribute_not_set;
            - if NotLoaded -- call Field.get_if_not_loaded;
            - call Field.from_mongo;
            - set __name__ and __parent__
            - save to __cache__

        4. Call Field.get_default;
        5. If AttributeNotSet -- call Field.get_if_attribute_not_set;
        6. Return value.
        """
        name = self.name

        if instance is None:
            return self.field

        elif name in instance.__changed__:
            value = instance.__changed__[name]

        elif name in instance.__cache__:
            value = instance.__cache__[name]

        elif name in instance.__raw__:
            raw = instance.__raw__[name]

            if raw is AttributeNotSet:
                return self.field.get_if_attribute_not_set(instance)
            if raw is NotLoaded:
                return self.field.get_if_not_loaded(instance)

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
            return self.field.get_if_attribute_not_set(instance)

        return value

    def __set__(self, instance, value):
        """ Set value to document.

        1. Call Field.prepare_value for cast value;
        2. Save in Document.__changed__;
        3. Call Field.set_parent_changed.
        """
        if not isinstance(instance, type):
            value = self.field.prepare_value(instance, value)

            name = self.name

            if name in instance.__changed__:
                value_old = instance.__changed__[name]
            elif name in instance.__cache__:
                value_old = instance.__cache__[name]
            elif name in instance.__raw__:
                try:
                    value_old = getattr(instance, name, AttributeNotSet)
                except NotLoadedError:
                    value_old = NotLoaded
            else:
                value_old = AttributeNotSet

            if value != value_old:
                instance.__changed__[name] = value
                self.field.set_parent_changed(instance)

        else:
            raise TypeError("can't set field directly")  # pragma: no cover

    def __delete__(self, instance):
        """ Mark document's key as not set.
        """
        if not isinstance(instance, type):
            setattr(instance, self.name, AttributeNotSet)


class Field:
    """ Base field for all database fields.

    :param bool smart_null:

        If it `True`, access to not exists fields return
        `None` instead `AttributeError` exception.
        You will not be able to distinguish null value from
        not exist. Use with care.

    .. py:attribute:: descriptor_class

        Class of desctiptor for work with field

    .. py:attribute:: document_class

        Class of document.
        Set in :py:meth:`contribute_to_class`.

    .. py:attribute:: name

        Name of field in document.
        Set in :py:meth:`contribute_to_class`.
    """
    descriptor_class = FieldDescriptor
    smart_null = False
    document_class = None
    name = None

    def __init__(self, smart_null=False):
        self.smart_null = smart_null

    def __repr__(self):
        class_name = type(self).__name__
        doc_class_name = self.document_class and self.document_class.__name__
        return '<{} "{}.{}">'.format(class_name, doc_class_name, self.name)

    def contribute_to_class(self, document_class, name):
        """ Add field for document_class.

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
        """ Return copy of field.
        """
        return self.__class__(smart_null=self.smart_null)

    def get_default(self, document):
        """ Return default value.
        """
        return AttributeNotSet

    def get_if_not_loaded(self, document):
        """ Call if field data marked as not loaded.
        """
        raise NotLoadedError(self, document)

    def get_if_attribute_not_set(self, document):
        """ Call if key not exist in document.
        """
        raise AttributeError(self.name)

    def get_fake(self, document, faker, deep):  # pragma: no cover
        """ Return fake data for testing.
        """
        return self.get_default(document)

    def prepare_value(self, document, value):  # pragma: no cover
        """ The method is called when value is assigned for the attribute.

        :param BaseDocument document: document
        :param value: raw value
        :return: prepared value

        It must be accept `value` argument and return
        processed (e.g. casted) analog. Also it is called
        once for the default value.
        """
        return value

    def to_mongo(self, document, value):  # pragma: no cover
        """ Convert python value to mongo value.

        :param BaseDocument document: document
        :param value: python value
        :return: mongo value
        """
        return value

    def from_mongo(self, document, value):  # pragma: no cover
        """ Convert mongo value to python value.

        :param BaseDocument document: document
        :param value: mongo value
        :return: python value
        """
        return value


class DefaultMixin:
    default = AttributeNotSet

    def __init__(self, *args, default=AttributeNotSet, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = default

    def get_default(self, document):
        return self.default

    def copy(self):
        """ Return copy of field.
        """
        return self.__class__(smart_null=self.smart_null, default=self.default)
