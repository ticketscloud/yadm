import decimal

import pytest

from yadm import fields
from yadm.fields.money import DEFAULT_CURRENCY_STORAGE
from yadm.documents import Document


def test_money_general():
    assert str(fields.Money(1, 'USD')) == '1.00 USD'
    assert str(fields.Money('1.2345', 840)) == '1.24 USD'
    assert str(fields.Money(10, 'RUB')) == '10.00 RUB'
    assert str(fields.Money('99', 'CHF')) == '99.00 CHF'

    assert str(fields.Money(5, 840)) == '5.00 USD'
    assert str(fields.Money(7, 826)) == '7.00 GBP'

    r1 = fields.Money(10, 'RUB')
    r2 = fields.Money(r1)
    assert r1 == r2
    assert r1 is not r2

    with pytest.raises(TypeError):
        fields.Money(r1, 'RUB')


def test_money_from_string():
    assert fields.Money.from_string('1.00 USD') == fields.Money(1, 'USD')
    assert fields.Money.from_string('  1.2345  USD') == fields.Money('1.2345', 'USD')
    assert fields.Money.from_string('1\tUSD\n') == fields.Money(1, 'USD')
    assert fields.Money.from_string('1 840') == fields.Money(1, 'USD')

    with pytest.raises(decimal.InvalidOperation):
        fields.Money.from_string('one USD')

    with pytest.raises(ValueError):
        fields.Money.from_string('1 A01')


def test_money_arithmetic_operations():
    r10 = fields.Money(10, 'RUB')
    r1 = fields.Money('1', 'RUB')
    u10 = fields.Money('10', 'USD')
    u1 = fields.Money(1, 'USD')

    assert r10 + r1 == fields.Money(11, 'RUB')
    assert r1 + r10 == fields.Money(11, 'RUB')
    assert r10 - r1 == fields.Money(9, 'RUB')
    assert r1 - r10 == fields.Money(-9, 'RUB')
    assert u10 + u1 == fields.Money(11, 'USD')
    assert u1 + u10 == fields.Money(11, 'USD')
    assert u10 - u1 == fields.Money(9, 'USD')
    assert u1 - u10 == fields.Money(-9, 'USD')

    assert r10 * 10 == fields.Money(100, 'RUB')
    assert 10 * r10 == fields.Money(100, 'RUB')
    assert r10 / 10 == fields.Money(1, 'RUB')
    assert u1 * 10 == fields.Money(10, 'USD')
    assert 10 * u1 == fields.Money(10, 'USD')
    assert u1 / 10 == fields.Money('0.1', 'USD')


def test_money_is_methods():
    r0 = fields.Money(0, 'RUB')
    r1 = fields.Money(1, 'RUB')
    r_1 = fields.Money(-1, 'RUB')

    assert r0.is_zero()
    assert r1.is_positive()
    assert r_1.is_negative()


def test_money_cmp():
    r0 = fields.Money(0, 'RUB')
    r1 = fields.Money(1, 'RUB')
    r_1 = fields.Money(-1, 'RUB')
    r10 = fields.Money(10, 'RUB')
    u1 = fields.Money(1, 'USD')

    assert r10 > r1
    assert not r10 < r1
    assert r1 < r10
    assert not r1 > r10
    assert r10 >= r1
    assert not r10 <= r1
    assert r1 <= r10
    assert not r1 >= r10

    assert r1 > 0
    assert not r1 < 0
    assert r_1 < 0
    assert not r_1 > 0

    assert not bool(r0)
    assert bool(r1)

    assert r0 == 0
    assert not r0 != 0

    with pytest.raises(ValueError) as e:
        r10 > u1

    with pytest.raises(TypeError) as e:
        r0 > 1
    assert 'unorderable types: Money() > int()' in str(e.value)


def test_money_forbidden_operations():
    r1 = fields.Money('1', 'RUB')
    u1 = fields.Money(1, 'USD')

    with pytest.raises(ValueError) as e:
        u1 + r1
    assert "Money '1.00 USD' and '1.00 RUB' must be with the same currencies" in str(e.value)

    with pytest.raises(TypeError) as e:
        u1 * r1
    assert "unsupported operand type(s) for *: 'Money' and 'Money'" in str(e.value)

    with pytest.raises(TypeError) as e:
        u1 + 10
    assert "unsupported operand type(s) for +: 'Money' and 'int'" in str(e.value)

    with pytest.raises(TypeError) as e:
        10 + u1
    assert "unsupported operand type(s) for +: 'int' and 'Money'" in str(e.value)


def test_currency_storage():
    assert DEFAULT_CURRENCY_STORAGE['USD'].code == 840
    assert DEFAULT_CURRENCY_STORAGE[840].string == 'USD'
    assert DEFAULT_CURRENCY_STORAGE['USD'].precision == 2
    assert DEFAULT_CURRENCY_STORAGE['RUB'].code == 643
    assert DEFAULT_CURRENCY_STORAGE[643].string == 'RUB'
    assert DEFAULT_CURRENCY_STORAGE['RUB'].precision == 2


@pytest.mark.parametrize('iv, ov', [
    ('3.14', '3.14'),
    ('0.14', '0.14'),
    ('30.14', '30.14'),
    ('3.1', '3.10'),
    ('0.1', '0.10'),
])
@pytest.mark.parametrize('cur', ['RUB', 'EUR', 'USD'])
def test_money_str(iv, cur, ov):
    assert str(fields.Money(iv, cur)) == '{} {}'.format(ov, cur)


@pytest.mark.parametrize('iv, ov', [
    ('3.14', 314),
    ('0.14', 14),
    ('30.14', 3014),
    ('3.1', 310),
    ('0.1', 10),
])
@pytest.mark.parametrize('cur', ['RUB', 'EUR', 'USD'])
@pytest.mark.parametrize('func', [
    lambda money: money.total_cents,
    lambda money: money.to_mongo()['v'],  # b/c function, for coverage
])
def test_money_cents(func, iv, cur, ov):
    assert func(fields.Money(iv, cur)) == ov


class Doc(Document):
    __collection__ = 'testdocs'
    money = fields.MoneyField(bbc='RUB')


def test_save_usd(db):
    doc = Doc()
    doc.money = fields.Money('3.14', 'USD')
    db.save(doc)

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], dict)
    assert data['money'] == {'c': 840, 'v': 314}


def test_save_clf(db):
    doc = Doc()
    doc.money = fields.Money(1, 'CLF')
    db.save(doc)

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], dict)
    assert data['money'] == {'c': 990, 'v': 10000}


def test_save_byr(db):
    doc = Doc()
    doc.money = fields.Money(1, 'BYR')
    db.save(doc)

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], dict)
    assert data['money'] == {'c': 974, 'v': 1}


def test_load_usd(db):
    db.db.testdocs.insert({'money': {'c': 840, 'v': 314}})

    doc = db(Doc).find_one()

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('3.14', 'USD')


def test_load_clf(db):
    db.db.testdocs.insert({'money': {'c': 990, 'v': 10000}})

    doc = db(Doc).find_one()

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('1', 'CLF')


def test_load_byr(db):
    db.db.testdocs.insert({'money': {'c': 974, 'v': 1}})

    doc = db(Doc).find_one()

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('1', 'BYR')


def test_load_and_save(db):
    db.db.testdocs.insert({'money': {'v': 999, 'c': 840}})

    doc = db(Doc).find_one()
    doc.money = fields.Money('10.5', 'RUB')
    db.save(doc)

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('10.5', 'RUB')

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], dict)
    assert data['money'] == {'v': 1050, 'c': 643}


def test_load_and_save_bbc(db):
    db.db.testdocs.insert({'money': 1234})

    doc = db(Doc).find_one()

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('12.34', 'RUB')
