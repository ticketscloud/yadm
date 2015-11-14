""" YADM with faker integration
"""

from faker import Faker

from yadm.documents import BaseDocument, Document, EmbeddedDocument
from yadm.markers import AttributeNotSet


DEFAULT_DEEP = 4  # <=450


def create_fake(__document_class__,
                __db__=None,
                __parent__=None,
                __name__=None,
                __faker__=None,
                __depth__=DEFAULT_DEEP,
                **values):
    """ Create document with fake data

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

    if doc_fake_proc is not None:
        values = next(doc_fake_proc)

    for name, field in __document_class__.__fields__.items():
        if name in values:
            fake = values[name]
        elif hasattr(document, '__fake__{}__'.format(name)):
            attr = getattr(document, '__fake__{}__'.format(name))
            fake = attr(__faker__, __depth__ - 1)
        else:
            fake = field.get_fake(document, __faker__, __depth__ - 1)

        if fake is not AttributeNotSet:
            setattr(document, name, fake)

    if doc_fake_proc is not None:
        try:
            next(doc_fake_proc)
        except StopIteration:
            doc_fake_proc = None

    if __db__ is not None:
        __db__.insert(document)

        if doc_fake_proc is not None:
            try:
                next(doc_fake_proc)
            except StopIteration:
                pass

    return document
