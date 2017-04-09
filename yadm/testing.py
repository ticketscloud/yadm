""" YADM with faker integration.
"""
from types import GeneratorType

import pymongo
from faker import Faker

from yadm.documents import BaseDocument, Document, EmbeddedDocument
from yadm.markers import AttributeNotSet


DEFAULT_DEPTH = 4  # <=450


def create_fake(__document_class__,
                __db__=None,
                __parent__=None,
                __name__=None,
                __faker__=None,
                __depth__=DEFAULT_DEPTH,
                __write_concern__=pymongo.WriteConcern(w='majority'),
                **values):
    """ Create document with fake data.

    :param yadm.documents.BaseDocument __document_class__: document class
        for new instance
    :param yadm.database.Database __db__: database instance
        if specified, document and all references will be saved to database
    :param yadm.documents.BaseDocument __parent__: parent document
    :param str __name__: name of parent field
    :param Faker __faker__: faker instance, create if not specified
    :param int __depth__: maximum recursion depth,
        not recomendated use greater than 450
        (default 4)
    :return yadm.documents.BaseDocument: __document_class__ instance with fake data
    """
    if not issubclass(__document_class__, BaseDocument):
        raise TypeError("only BaseDocument subclasses is allowed")

    if __depth__ < 0:
        return AttributeNotSet

    if __faker__ is None:
        __faker__ = Faker()

    document = __document_class__()

    if isinstance(document, Document):
        document.__db__ = __db__
    elif isinstance(document, EmbeddedDocument):
        document.__parent__ = __parent__
        document.__name__ = __name__

    doc_fake_proc = document.__fake__(values, __faker__, __depth__ - 1)

    # extend values from __fake__ method
    if isinstance(doc_fake_proc, GeneratorType):
        values = next(doc_fake_proc)
    elif isinstance(doc_fake_proc, dict):
        values = doc_fake_proc

    # first: set values
    for name, fake in values.items():
        if fake is not AttributeNotSet:
            setattr(document, name, fake)

    # second: field faker
    for name, field in __document_class__.__fields__.items():
        if name not in values and not hasattr(document, '__fake__{}__'.format(name)):
            fake = field.get_fake(document, __faker__, __depth__ - 1)
            if fake is not AttributeNotSet:
                setattr(document, name, fake)

    # third: __fake__{name}__ methods
    for name, field in __document_class__.__fields__.items():
        if name not in values and hasattr(document, '__fake__{}__'.format(name)):
            attr = getattr(document, '__fake__{}__'.format(name))
            fake = attr(__faker__, __depth__ - 1)

            if fake is not AttributeNotSet:
                setattr(document, name, fake)

    if isinstance(doc_fake_proc, GeneratorType):
        # pre save processor
        try:
            next(doc_fake_proc)
        except StopIteration:
            doc_fake_proc = None

    if __db__ is not None:
        __db__.insert(document, write_concern=__write_concern__)

        # post save processor
        if isinstance(doc_fake_proc, GeneratorType):
            try:
                next(doc_fake_proc)
            except StopIteration:
                pass

    return document
