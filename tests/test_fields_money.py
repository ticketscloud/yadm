from yadm import fields
from yadm.documents import Document


def test_money_str():
    assert str(fields.Money('3.14')) == '3.14'
    assert str(fields.Money('0.14')) == '0.14'
    assert str(fields.Money('30.14')) == '30.14'
    assert str(fields.Money('3.1')) == '3.10'
    assert str(fields.Money('0.1')) == '0.10'


def test_money_to_mongo():
    assert fields.Money('3.14').to_mongo() == 314
    assert fields.Money('0.14').to_mongo() == 14
    assert fields.Money('30.14').to_mongo() == 3014
    assert fields.Money('3.1').to_mongo() == 310
    assert fields.Money('0.1').to_mongo() == 10


class TestDoc(Document):
    __collection__ = 'testdocs'
    money = fields.MoneyField()


def test_save(db):  # noqa
    doc = TestDoc()
    doc.money = fields.Money('3.14')
    db.save(doc)

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], int)
    assert data['money'] == 314


def test_load(db):
    db.db.testdocs.insert({'money': 314})

    doc = db.get_queryset(TestDoc).find_one()

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('3.14')


def test_load_and_save(db):
    db.db.testdocs.insert({'money': 314})

    doc = db.get_queryset(TestDoc).find_one()
    doc.money = fields.Money('42.00')
    db.save(doc)

    assert hasattr(doc, 'money')
    assert isinstance(doc.money, fields.Money)
    assert doc.money == fields.Money('42.00')

    data = db.db.testdocs.find_one()

    assert 'money' in data
    assert isinstance(data['money'], int)
    assert data['money'] == 4200
