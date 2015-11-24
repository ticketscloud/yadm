import pytest

from yadm import (
    Document,
    EmbeddedDocument,
)
from yadm.common import EnclosedDocDescriptor
from yadm.fields import (
    Field,
    BooleanField,
    IntegerField,
    FloatField,
    StringField,
    EmbeddedDocumentField,
    ReferenceField,
)


class Foo(Document):
    f = BooleanField(True)


class Bar(EmbeddedDocument):
    f = IntegerField(42)


class Baz(Document):
    __collection__ = 'baz'
    f = FloatField(3.14)


class Qux(EmbeddedDocument):
    f = StringField('quux')


class Corge(Document):
    grault = EmbeddedDocumentField(Foo)
    garply = ReferenceField(Bar)
    waldo = EmbeddedDocumentField(Baz)
    fred = ReferenceField(Qux)
    plugh = EmbeddedDocumentField('self')
    xyzzy = ReferenceField('self')


class Thud(Document):
    rf = ReferenceField('dumb.path')


class Thud2(EmbeddedDocument):
    ef = EmbeddedDocumentField('another.dumb.path')


class FooDocField(Field):
    reference_document_class = EnclosedDocDescriptor('reference')
    embedded_document_class = EnclosedDocDescriptor('embedded')


@pytest.fixture(scope='function')
def fs(request):
    class FieldsSet:
        R = ReferenceField(Foo)
        E = EmbeddedDocumentField(Bar)
        CR = ReferenceField('tests.test_common.Baz')
        CE = EmbeddedDocumentField('tests.test_common.Qux')

    return FieldsSet


def assert_reference(classinfo, expected_value):
    assert classinfo.reference_document_class == expected_value
    assert getattr(classinfo, '_reference_document_class') == expected_value


def assert_embedded(classinfo, expected_value):
    assert classinfo.embedded_document_class == expected_value
    assert getattr(classinfo, '_embedded_document_class') == expected_value


def test_reference(fs):
    assert_reference(fs.R, Foo)


def test_embedded(fs):
    assert_embedded(fs.E, Bar)


def test_resolving(fs):
    assert_reference(fs.CR, Baz)
    assert_embedded(fs.CE, Qux)

    assert_embedded(Corge.grault, Foo)
    assert_reference(Corge.garply, Bar)
    assert_embedded(Corge.waldo, Baz)
    assert_reference(Corge.fred, Qux)
    assert_embedded(Corge.plugh, Corge)
    assert_reference(Corge.xyzzy, Corge)

    with pytest.raises(ImportError):
        Thud.rf.reference_document_class

    with pytest.raises(ImportError):
        Thud2.ef.embedded_document_class


def test_lowlevel(fs):
    EnclosedDocDescriptor('reference')
    EnclosedDocDescriptor('embedded')

    for cls_type in ('waldo', 'fred'):
        with pytest.raises(ValueError):
            EnclosedDocDescriptor(cls_type)

    # class binding
    assert isinstance(FooDocField.reference_document_class,
                      EnclosedDocDescriptor)
    assert isinstance(FooDocField.embedded_document_class,
                      EnclosedDocDescriptor)

    # getattr --> None
    doc_field = FooDocField()
    assert doc_field.reference_document_class is None
    assert doc_field.embedded_document_class is None

    doc_field.reference_document_class = Foo
    doc_field.embedded_document_class = Bar

    assert_reference(doc_field, Foo)
    assert_embedded(doc_field, Bar)

    # destructor
    del doc_field.reference_document_class
    assert getattr(doc_field, '_reference_document_class', None) is None
    del doc_field.embedded_document_class
    assert getattr(doc_field, '_embedded_document_class', None) is None


def test_redefine_notloaded(db):
    baz = Baz(f=13.0)
    db.save(baz)
    baz = db(Baz).fields('_id').find_one({'f': 13.0})
    baz.f = 666.0
    db.save(baz)
