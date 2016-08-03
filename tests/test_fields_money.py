import pytest

from yadm import fields
from yadm.documents import Document


@pytest.mark.parametrize('input, output', [
    ('3.14', '3.14'),
    ('0.14', '0.14'),
    ('30.14', '30.14'),
    ('3.1', '3.10'),
    ('0.1', '0.10'),
])
def test_money_str(input, output):
    assert str(fields.Money(input)) == output


@pytest.mark.parametrize('input, output', [
    ('3.14', 314),
    ('0.14', 14),
    ('30.14', 3014),
    ('3.1', 310),
    ('0.1', 10),
])
@pytest.mark.parametrize('func', [
    lambda money: money.total_cents,
    lambda money: money.to_mongo(),  # b/c function, for coverage
])
def test_money_cents(func, input, output):
    assert func(fields.Money(input)) == output


class Doc(Document):
    __collection__ = 'testdocs'
    money = fields.MoneyField()


def test_save(db):  # noqa
    doc = Doc()
    doc.money = fields.Money('3.14')
    db.save(doc)

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], int)
    assert data['money'] == 314


def test_load(db):
    db.db.testdocs.insert({'money': 314})

    doc = db.get_queryset(Doc).find_one()

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('3.14')


def test_load_and_save(db):
    db.db.testdocs.insert({'money': 314})

    doc = db.get_queryset(Doc).find_one()
    doc.money = fields.Money('42.00')
    db.save(doc)

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('42.00')

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], int)
    assert data['money'] == 4200
