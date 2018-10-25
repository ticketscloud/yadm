"""
Base classes for build database fields.
"""
import functools

from yadm.exceptions import NotLoadedError
from yadm.markers import AttributeNotSet
from yadm.document_item import DocumentItemMixin
from yadm.log_items import SetField, ChangeChild


def pass_null(method):
    @functools.wraps(method)
    def wrapper(self, document, value):
        if value is None:
            return None  # pragma: no cover
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

        1. Lookup in __cache__;
        2. Lookup in __raw__;
        3. Lookup in __not_loaded__;
        4. Field.get_if_attribute_not_set();

        ...

        1. Lookup in __cache__:

            - if AttributeNotSet: Field.get_if_attribute_not_set();
            - return;

        2. Lookup in __raw__:

            - Field.from_mongo();
            - if DocumentItemMixin -- set __name__ and __parent__;
            - save to __cache__;
            - return;

        3. Lookup in __not_loaded__:

            - Fiels.get_if_not_loaded();
            - if AttributeNotSet: Field.get_if_attribute_not_set();
            - return;

        4. return Field.get_if_attribute_not_set();
        """
        name = self.name

        if instance is None:
            return self.field

        elif name in instance.__cache__:
            value = instance.__cache__[name]
            if value is not AttributeNotSet:
                return value
            else:
                return self.field.get_if_attribute_not_set(instance)

        elif name in instance.__raw__:
            value = self.field.from_mongo(instance, instance.__raw__[name])

            if isinstance(value, DocumentItemMixin):
                value.__name__ = self.field.name
                value.__parent__ = instance

            instance.__cache__[name] = value
            return value

        elif name in instance.__not_loaded__:
            return self.field.get_if_not_loaded(instance)

        else:
            return self.field.get_if_attribute_not_set(instance)

    def __set__(self, instance, value):
        """ Set value to document.

        1. Call Field.prepare_value for cast value;
        2. Save in Document.__cache__;
        """
        if not isinstance(instance, type):
            value = self.field.prepare_value(instance, value)

            name = self.name

            if value is AttributeNotSet:
                instance.__cache__[name] = AttributeNotSet
                set_field_log_item = SetField(name=self.name, value=value)
                instance.__log__.append(set_field_log_item)

                if isinstance(instance, DocumentItemMixin):
                    root = instance.__document__
                    if root is not None:
                        root.__log__.append(
                            ChangeChild(
                                path=instance.__field_name__,
                                name=self.name,
                                log_item=set_field_log_item,
                            ),
                        )

            else:
                if name in instance.__cache__:
                    value_old = instance.__cache__[name]
                elif name in instance.__raw__:
                    value_old = getattr(instance, name)
                else:
                    value_old = AttributeNotSet

                if value != value_old:
                    instance.__cache__[name] = value
                    set_field_log_item = SetField(name=self.name, value=value)
                    instance.__log__.append(set_field_log_item)

                    if isinstance(instance, DocumentItemMixin):
                        root = instance.__document__
                        if root is not None:
                            root.__log__.append(
                                ChangeChild(
                                    path=instance.__field_name__,
                                    name=self.name,
                                    log_item=set_field_log_item,
                                ),
                            )

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

    def copy(self):  # pragma: no cover
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
        raise AttributeError("{!r} document has no attribute {!r}"
                             "".format(document.__class__.__name__, self.name))

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
