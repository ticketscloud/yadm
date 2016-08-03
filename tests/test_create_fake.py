import pytest

from yadm.testing import create_fake
from yadm.documents import Document
from yadm import fields


class DocRef(Document):
    __collection__ = 'testdocs_ref'
    s = fields.StringField()


class Doc(Document):
    __collection__ = 'testdocs'
    ref = fields.ReferenceField(DocRef)
    i = fields.IntegerField()


def test_reference(db):
    doc = create_fake(Doc, db)
    assert isinstance(doc, Doc)
    assert isinstance(doc.ref, DocRef)
    assert doc.ref.id
