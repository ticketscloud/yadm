"""
Field for money

Work with `decimal.Decimal` and store value as integer.
Use :class:`yadm.fields.money.Money`, as value for money.

.. code block: python

    class DocClass(Document):
        money = MoneyField()

    doc = DocClass()
    doc.money = Money('3.14')

    db.insert(doc)

This code save to MongoDB document:

.. code block: javascript

    {
        id: ObjectId('534272984c78591787e1a964'),
        money: 314
    }


"""
from decimal import Decimal, Context, ROUND_UP

from yadm.fields.base import Field, DefaultMixin
from yadm.markers import AttributeNotSet


class Money(Decimal):
    context = Context(rounding=ROUND_UP)

    def __new__(cls, value):
        return Decimal.__new__(cls, value, cls.context)

    def to_mongo(self):
        return int(self.quantize(Decimal('1.00'), ROUND_UP) * 100)

    def __str__(self):
        s = str(self.to_mongo()).rjust(3, '0')
        return '.'.join((s[:-2], s[-2:]))

    def __xor__(self, other):
        return NotImplemented

    def __rxor__(self, other):
        return NotImplemented

    def __mod__(self, other):
        return NotImplemented

    def __rmod__(self, other):
        return NotImplemented

    def __divmod__(self, other):
        return NotImplemented

    def __rdivmod__(self, other):
        return NotImplemented


class MoneyField(DefaultMixin, Field):
    """ Field for work with money
    """
    def get_fake(self, document, faker, depth):
        return Money(faker.pydecimal(
            left_digits=5, right_digits=2, positive=True))

    def prepare_value(self, document, value):
        """ Cast value to :class:`decimal.Decimal`
        """
        if value is None or value is AttributeNotSet:
            return None

        elif isinstance(value, Money):
            return value

        elif isinstance(value, (str, Decimal)):
            return Money(value)

        else:
            raise TypeError(repr(value))

    def to_mongo(self, document, value):
        if value is None:
            return None

        elif isinstance(value, (str, Decimal)):
            return Money(value).to_mongo()

        elif isinstance(value, Money):
            return value.to_mongo()

        elif isinstance(value, int):
            return value

        else:
            raise TypeError(repr(value))

    def from_mongo(self, document, data):
        if isinstance(data, int):
            return Money(data / Decimal(100))
        else:
            return self.prepare_value(document, data)
