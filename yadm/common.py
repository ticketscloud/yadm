""" Common part for working with imports, documents and so on
"""
from collections.abc import Mapping

from zope.dottedname.resolve import resolve


class EnclosedDocDescriptor:
    """ Descriptor for accessing an enclosed documens within an embedded
    (:py:class:`yadm.fields.embedded.EmbeddedDocumentField`) and a reference
    (:py:class:`yadm.fields.reference.ReferenceField`) fields.

    :param str enclosed_cls_type: Enclosed class type. Can take `embedded` or
        `reference` value. Otherwise :py:exc:`ValueError` will be raised.
    """

    _DOC_CLS = 'document_class'
    _RECURSIVE_REF_CONST = 'self'

    def __init__(self, enclosed_cls_type):
        if enclosed_cls_type in ('embedded', 'reference'):
            self.attr_name = '_{}_{}'.format(enclosed_cls_type, self._DOC_CLS)
        else:
            raise ValueError

    def __get__(self, instance, owner):
        if not instance:
            return self

        value = getattr(instance, self.attr_name, None)

        if isinstance(value, str):
            if self._RECURSIVE_REF_CONST == value:
                value = getattr(instance, self._DOC_CLS)
            else:
                value = resolve(value)
            self.__set__(instance, value)

        return value

    def __set__(self, instance, value):
        setattr(instance, self.attr_name, value)

    def __delete__(self, instance):
        delattr(instance, self.attr_name)


class BaseResult(Mapping):
    """ Base class for operations results
    """
    def __init__(self, raw):
        self._raw = raw

    def __getitem__(self, item):
        return self._raw[item]

    def __iter__(self):
        return iter(self._raw)

    def __len__(self):
        return len(self._raw)
