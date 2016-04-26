import pytest

from yadm.testing import create_fake
from yadm.documents import Document
from yadm import fields


class TestDocRef(Document):
    __collection__ = 'testdocs_ref'
    s = fields.StringField()


class TestDoc(Document):
    __collection__ = 'testdocs'
    ref = fields.ReferenceField(TestDocRef)
    i = fields.IntegerField()


def test_reference(db):
    doc = create_fake(TestDoc, db)
    assert isinstance(doc, TestDoc)
    assert isinstance(doc.ref, TestDocRef)
    assert doc.ref.id
