from collections import Counter
from types import GeneratorType, CoroutineType

import pymongo
from faker import Faker

from yadm.documents import BaseDocument, Document, EmbeddedDocument
from yadm.markers import AttributeNotSet
from yadm.testing import DEFAULT_DEPTH


async def aio_create_fake(__document_class__,
                          __db__=None,
                          __faker__=None,
                          *,
                          __parent__=None,
                          __name__=None,
                          __depth__=DEFAULT_DEPTH,
                          __write_concern__=pymongo.WriteConcern(w='majority'),
                          **values):
    if not issubclass(__document_class__, BaseDocument):  # pragma: no cover
        raise TypeError("only BaseDocument subclasses is allowed")

    aio_create_fake.counter[__document_class__] += 1

    if __depth__ < 0:
        return AttributeNotSet

    if __faker__ is None:
        aio_create_fake.counter['__without_faker__'] += 1
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

            if isinstance(fake, CoroutineType):
                fake = await fake
            if fake is not AttributeNotSet:
                setattr(document, name, fake)

    # third: __fake__{name}__ methods
    for name, field in __document_class__.__fields__.items():
        if name not in values and hasattr(document, '__fake__{}__'.format(name)):
            attr = getattr(document, '__fake__{}__'.format(name))
            fake = attr(__faker__, __depth__ - 1)
            if isinstance(fake, CoroutineType):
                fake = await fake

            if fake is not AttributeNotSet:
                setattr(document, name, fake)

    if isinstance(doc_fake_proc, GeneratorType):
        # pre save processor
        try:
            next(doc_fake_proc)
        except StopIteration:
            doc_fake_proc = None

    if __db__ is not None:
        await __db__.insert_one(document, write_concern=__write_concern__)

        # post save processor
        if isinstance(doc_fake_proc, GeneratorType):
            try:
                next(doc_fake_proc)
            except StopIteration:
                pass

    return document


aio_create_fake.counter = Counter()
