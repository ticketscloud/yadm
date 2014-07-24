import random
from unittest import TestCase

from decimal import Decimal, Context, getcontext

from yadm import fields
from yadm.documents import Document

from .test_database import BaseDatabaseTest


class DecimalFieldUnitTest(TestCase):
    def test_to_mongo(self):
        field = fields.DecimalField()

        data = field.to_mongo(None, Decimal('123.45'))
        self.assertEqual(set(data.keys()), {'i', 'e'})
        self.assertEqual(data['i'], 12345)
        self.assertEqual(data['e'], -2)

    def test_from_mongo(self):
        field = fields.DecimalField()

        dec = field.from_mongo(None, {'i': 12345, 'e': -2})
        self.assertEqual(dec, Decimal('123.45'))

    def test_integer_from_digits(self):
        for _ in range(100):
            digits = [random.randint(0, 9) for _ in range(random.randint(5, 20))]
            integer = fields.DecimalField._integer_from_digits(digits)

        self.assertEqual(integer, int(''.join(str(i) for i in digits)))

    def test_func_int(self):
        field = fields.DecimalField()
        dec = field.prepare_value(None, 13)
        self.assertIsInstance(dec, Decimal)
        self.assertEqual(dec, Decimal(13))

    def test_func_str(self):
        field = fields.DecimalField()
        dec = field.prepare_value(None, '3.14')
        self.assertIsInstance(dec, Decimal)
        self.assertEqual(dec, Decimal('3.14'))

    def test_func_error(self):
        field = fields.DecimalField()
        self.assertRaises(TypeError, field.prepare_value, type)

    def test_context_default(self):
        field = fields.DecimalField()
        self.assertIsInstance(field.context, Context)
        self.assertEqual(field.context, getcontext())

    def test_context_init(self):
        context = Context(prec=5)
        field = fields.DecimalField(context=context)
        self.assertIsInstance(field.context, Context)
        self.assertEqual(field.context, context)


class DecimalFieldTest(BaseDatabaseTest):
    def setUp(self):
        super().setUp()

        class TestDoc(Document):
            __collection__ = 'testdocs'
            dec = fields.DecimalField()

        self.TestDoc = TestDoc

    def test_save(self):
        td = self.TestDoc()
        td.dec = Decimal('3.14')
        self.db.save(td)

        data = self.db.db.testdocs.find_one()

        self.assertIn('dec', data)
        self.assertEqual(set(data['dec']), {'i', 'e'})
        self.assertEqual(data['dec']['i'], 314)
        self.assertEqual(data['dec']['e'], -2)

    def test_load(self):
        self.db.db.testdocs.insert({'dec': {'i': 314, 'e': -2}})

        doc = self.db.get_queryset(self.TestDoc).find_one()

        self.assertTrue(hasattr(doc, 'dec'))
        self.assertIsInstance(doc.dec, Decimal)
        self.assertEqual(doc.dec, Decimal('3.14'))
