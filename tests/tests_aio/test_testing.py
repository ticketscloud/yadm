import pytest
from bson import ObjectId

from yadm.documents import Document, EmbeddedDocument
from yadm.markers import AttributeNotSet
from yadm.aio.testing import aio_create_fake as create_fake
from yadm.fields import (
    BooleanField, StringField, IntegerField, ObjectIdField, EmailField,
    EmbeddedDocumentField, ReferenceField,
)


class SimpleDoc(Document):
    __collection__ = 'testdocs'

    oid = ObjectIdField()
    b = BooleanField()
    s = StringField()
    i = IntegerField()
    e = EmailField()
    email = StringField()
    not_set = StringField()

    def __fake__email__(self, faker, depth):
        return faker.email()

    def __fake__not_set__(self, faker, depth):
        return AttributeNotSet


class EmbeddedDoc(EmbeddedDocument):
    __collection__ = 'testdocs'

    name = StringField()


@pytest.mark.asyncio()
async def test_simple(db):
    doc = await create_fake(SimpleDoc)

    assert doc.__db__ is None

    assert hasattr(doc, 'oid')
    assert hasattr(doc, 'b')
    assert hasattr(doc, 's')
    assert hasattr(doc, 'i')
    assert hasattr(doc, 'e')
    assert hasattr(doc, 'email')
    assert not hasattr(doc, 'not_set')

    assert isinstance(doc.oid, ObjectId)
    assert isinstance(doc.b, bool)
    assert isinstance(doc.s, str)
    assert isinstance(doc.i, int)
    assert isinstance(doc.e, str)
    assert isinstance(doc.email, str)

    assert len(doc.s) > 0
    assert '@' in doc.e
    assert '@' in doc.email

    assert await db(SimpleDoc).count_documents() == 0


@pytest.mark.asyncio()
async def test_simple_save(db):
    doc = await create_fake(SimpleDoc, __db__=db)

    assert doc.__db__ is db

    assert hasattr(doc, 'oid')
    assert hasattr(doc, 'b')
    assert hasattr(doc, 's')
    assert hasattr(doc, 'i')
    assert not hasattr(doc, 'not_set')

    assert isinstance(doc.oid, ObjectId)
    assert isinstance(doc.b, bool)
    assert isinstance(doc.s, str)
    assert isinstance(doc.i, int)

    assert len(doc.s) > 0

    assert await db(SimpleDoc).count_documents() == 1
    assert await db(SimpleDoc).find_one(doc.id)


class WithEmbeddedDoc(Document):
    __collection__ = 'testdocs'

    emb = EmbeddedDocumentField(EmbeddedDoc)


@pytest.mark.asyncio()
async def test_embedded():
    doc = await create_fake(WithEmbeddedDoc)

    assert hasattr(doc, 'emb')
    assert isinstance(doc.emb, EmbeddedDoc)
    assert isinstance(doc.emb.name, str)


@pytest.mark.asyncio()
async def test_embedded_depth_limit():
    doc = await create_fake(WithEmbeddedDoc, __depth__=0)

    assert hasattr(doc, 'emb')
    assert not hasattr(doc.emb, 'name')


class WithReferenceDoc(Document):
    __collection__ = 'with_ref_testdocs'

    ref = ReferenceField('tests.test_testing.SimpleDoc')


@pytest.mark.asyncio()
async def test_reference(db):
    doc = await create_fake(WithReferenceDoc)

    assert hasattr(doc, 'ref')
    assert hasattr(doc.ref, 's')
    assert isinstance(doc.ref.s, str)

    assert await db(WithReferenceDoc).count_documents() == 0
    assert await db(SimpleDoc).count_documents() == 0


@pytest.mark.asyncio()
async def test_reference_save(db):
    doc = await create_fake(WithReferenceDoc, __db__=db)

    assert hasattr(doc, 'ref')
    assert hasattr(await doc.ref, 's')
    assert isinstance((await doc.ref).s, str)

    assert await db(WithReferenceDoc).count_documents() == 1
    assert await db(SimpleDoc).count_documents() == 1


class WithReferenceCircleDoc(Document):
    __collection__ = 'testdocs'

    self = ReferenceField('tests.test_testing.WithReferenceCircleDoc')


@pytest.mark.asyncio()
async def test_reference_circle(db):
    doc = await create_fake(WithReferenceCircleDoc, __db__=db, __depth__=2)

    assert await db(WithReferenceCircleDoc).count_documents() == 3

    assert hasattr(doc, 'self')
    assert hasattr(await doc.self, 'self')
    assert not hasattr(await (await doc.self).self, 'self')


@pytest.mark.asyncio()
async def test_complex_fake_generator():
    class Doc(Document):
        i = IntegerField()
        s = StringField()

        def __fake__(self, values, faker, depth):
            assert values['i'] == 13
            assert 's' not in values
            new_values = values.copy()
            new_values['i'] += 1
            yield new_values
            self.s = 'string'
            yield

    doc = await create_fake(Doc, i=13)

    assert doc.i == 14
    assert doc.s == 'string'


@pytest.mark.asyncio()
async def test_complex_fake_dict():
    class Doc(Document):
        i = IntegerField()
        s = StringField()

        def __fake__(self, values, faker, depth):
            assert values['i'] == 13
            assert 's' not in values
            new_values = values.copy()
            new_values['i'] += 1
            new_values['s'] = 'string'
            return new_values

    doc = await create_fake(Doc, i=13)

    assert doc.i == 14
    assert doc.s == 'string'
