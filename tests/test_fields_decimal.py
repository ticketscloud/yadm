import random

from decimal import Decimal, Context, getcontext

from yadm import fields
from yadm.documents import Document


def test_to_mongo():
    field = fields.DecimalField()

    data = field.to_mongo(None, Decimal('123.45'))
    assert set(data.keys()) == {'i', 'e'}
    assert data['i'] == 12345
    assert data['e'] == -2


def test_from_mongo():
    field = fields.DecimalField()

    dec = field.from_mongo(None, {'i': 12345, 'e': -2})
    assert dec == Decimal('123.45')


def test_integer_from_digits():
    for _ in range(100):
        digits = [random.randint(0, 9) for _ in range(random.randint(5, 20))]
        integer = fields.DecimalField._integer_from_digits(digits)

    assert integer == int(''.join(str(i) for i in digits))


def test_func_int():
    field = fields.DecimalField()
    dec = field.prepare_value(None, 13)
    assert isinstance(dec, Decimal)
    assert dec == Decimal(13)


def test_func_str():
    field = fields.DecimalField()
    dec = field.prepare_value(None, '3.14')
    assert isinstance(dec, Decimal)
    assert dec == Decimal('3.14')


def test_context_default():
    field = fields.DecimalField()
    assert isinstance(field.context, Context)
    assert field.context == getcontext()


def test_context_init():
    context = Context(prec=5)
    field = fields.DecimalField(context=context)
    assert isinstance(field.context, Context)
    assert field.context == context


class Doc(Document):
    __collection__ = 'testdocs'
    dec = fields.DecimalField()


def test_save(db):
    doc = Doc()
    doc.dec = Decimal('3.14')
    db.save(doc)

    data = db.db.testdocs.find_one()

    assert 'dec' in data
    assert set(data['dec']) == {'i', 'e'}
    assert data['dec']['i'] == 314
    assert data['dec']['e'] == -2


def test_load(db):
    db.db.testdocs.insert({'dec': {'i': 314, 'e': -2}})

    doc = db.get_queryset(Doc).find_one()

    assert hasattr(doc, 'dec')
    assert isinstance(doc.dec, Decimal)
    assert doc.dec == Decimal('3.14')
