from unittest import TestCase

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


class EnclosedDocDescriptorTest(TestCase):

    def setUp(self):
        self.R = ReferenceField(Foo)
        self.E = EmbeddedDocumentField(Bar)
        self.CR = ReferenceField('tests.test_common.Baz')
        self.CE = EmbeddedDocumentField('tests.test_common.Qux')

    def assertReference(self, classinfo, expected_value):  # noqa
        self.assertEquals(classinfo.reference_document_class, expected_value)
        self.assertEquals(getattr(classinfo, '_reference_document_class'),
                          expected_value)

    def assertEmbedded(self, classinfo, expected_value):  # noqa
        self.assertEquals(classinfo.embedded_document_class, expected_value)
        self.assertEquals(getattr(classinfo, '_embedded_document_class'),
                          expected_value)

    def test_reference(self):
        self.assertReference(self.R, Foo)

    def test_embedded(self):
        self.assertEmbedded(self.E, Bar)

    def test_resolving(self):
        self.assertReference(self.CR, Baz)
        self.assertEmbedded(self.CE, Qux)

        self.assertEmbedded(Corge.grault, Foo)
        self.assertReference(Corge.garply, Bar)
        self.assertEmbedded(Corge.waldo, Baz)
        self.assertReference(Corge.fred, Qux)
        self.assertEmbedded(Corge.plugh, Corge)
        self.assertReference(Corge.xyzzy, Corge)

        with self.assertRaises(ImportError):
            Thud.rf.reference_document_class

        with self.assertRaises(ImportError):
            Thud2.ef.embedded_document_class

    def test_lowlevel(self):
        # wrong enclosed_cls_type value
        try:
            EnclosedDocDescriptor('reference')
            EnclosedDocDescriptor('embedded')
        except Exception as e:
            self.assertTrue(False, 'Unexpected exception: {!r}'.format(e))
        for cls_type in ('waldo', 'fred'):
            with self.assertRaises(ValueError):
                EnclosedDocDescriptor(cls_type)

        # class binding
        self.assertIsInstance(FooDocField.reference_document_class,
                              EnclosedDocDescriptor)
        self.assertIsInstance(FooDocField.embedded_document_class,
                              EnclosedDocDescriptor)

        # getattr --> None
        doc_field = FooDocField()
        self.assertIsNone(doc_field.reference_document_class)
        self.assertIsNone(doc_field.embedded_document_class)

        doc_field.reference_document_class = Foo
        doc_field.embedded_document_class = Bar

        self.assertReference(doc_field, Foo)
        self.assertEmbedded(doc_field, Bar)

        # destructor
        del doc_field.reference_document_class
        self.assertIsNone(getattr(doc_field, '_reference_document_class', None))
        del doc_field.embedded_document_class
        self.assertIsNone(getattr(doc_field, '_embedded_document_class', None))
