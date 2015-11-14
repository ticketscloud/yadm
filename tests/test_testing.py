from bson import ObjectId

from yadm.documents import Document, EmbeddedDocument
from yadm.testing import create_fake
from yadm.fields import (
    BooleanField, StringField, IntegerField, ObjectIdField, EmailField,
    EmbeddedDocumentField, ReferenceField,
)


class SimpleTestDoc(Document):
    __collection__ = 'testdocs'

    oid = ObjectIdField()
    b = BooleanField()
    s = StringField()
    i = IntegerField()
    e = EmailField()
    email = StringField()


class EmbeddedTestDoc(EmbeddedDocument):
    __collection__ = 'testdocs'

    name = StringField()


def test_simple(db):
    doc = create_fake(SimpleTestDoc)

    assert doc.__db__ is None

    assert hasattr(doc, 'oid')
    assert hasattr(doc, 'b')
    assert hasattr(doc, 's')
    assert hasattr(doc, 'i')
    assert hasattr(doc, 'e')
    assert hasattr(doc, 'email')

    assert isinstance(doc.oid, ObjectId)
    assert isinstance(doc.b, bool)
    assert isinstance(doc.s, str)
    assert isinstance(doc.i, int)
    assert isinstance(doc.e, str)
    assert isinstance(doc.email, str)

    assert len(doc.s) > 0
    assert '@' in doc.e
    assert '@' in doc.email

    assert db(SimpleTestDoc).count() == 0


def test_simple_save(db):
    doc = create_fake(SimpleTestDoc, __db__=db)

    assert doc.__db__ is db

    assert hasattr(doc, 'oid')
    assert hasattr(doc, 'b')
    assert hasattr(doc, 's')
    assert hasattr(doc, 'i')

    assert isinstance(doc.oid, ObjectId)
    assert isinstance(doc.b, bool)
    assert isinstance(doc.s, str)
    assert isinstance(doc.i, int)

    assert len(doc.s) > 0

    assert db(SimpleTestDoc).count() == 1
    assert db(SimpleTestDoc).with_id(doc.id)


class WithEmbeddedTestDoc(Document):
    __collection__ = 'testdocs'

    emb = EmbeddedDocumentField(EmbeddedTestDoc)


def test_embedded(db):
    doc = create_fake(WithEmbeddedTestDoc)

    assert hasattr(doc, 'emb')
    assert isinstance(doc.emb, EmbeddedTestDoc)
    assert isinstance(doc.emb.name, str)


def test_embedded_depth_limit(db):
    doc = create_fake(WithEmbeddedTestDoc, __depth__=0)

    assert hasattr(doc, 'emb')
    assert not hasattr(doc.emb, 'name')


class WithReferenceTestDoc(Document):
    __collection__ = 'with_ref_testdocs'

    ref = ReferenceField('tests.test_testing.SimpleTestDoc')


def test_reference(db):
    doc = create_fake(WithReferenceTestDoc)

    assert hasattr(doc, 'ref')
    assert hasattr(doc.ref, 's')
    assert isinstance(doc.ref.s, str)

    assert db(WithReferenceTestDoc).count() == 0
    assert db(SimpleTestDoc).count() == 0


def test_reference_save(db):
    doc = create_fake(WithReferenceTestDoc, __db__=db)

    assert hasattr(doc, 'ref')
    assert hasattr(doc.ref, 's')
    assert isinstance(doc.ref.s, str)

    assert db(WithReferenceTestDoc).count() == 1
    assert db(SimpleTestDoc).count() == 1


class WithReferenceCircleTestDoc(Document):
    __collection__ = 'testdocs'

    self = ReferenceField('tests.test_testing.WithReferenceCircleTestDoc')


def test_reference_circle(db):
    doc = create_fake(WithReferenceCircleTestDoc, __db__=db, __depth__=2)

    assert db(WithReferenceCircleTestDoc).count() == 3

    assert hasattr(doc, 'self')
    assert hasattr(doc.self, 'self')
    assert not hasattr(doc.self.self, 'self')
