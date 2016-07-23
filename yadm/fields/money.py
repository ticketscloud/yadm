"""
Field for money.

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

from yadm.fields.base import Field, DefaultMixin, pass_null
from yadm.markers import AttributeNotSet


class Money(Decimal):
    context = Context(rounding=ROUND_UP)

    def __new__(cls, value):
        return Decimal.__new__(cls, value, cls.context)

    @classmethod
    def from_cents(cls, cents: int):
        """ Return new Money object from cents.
        """
        return cls(cents / Decimal(100))

    @property
    def total_cents(self) -> int:
        """ Return total cents in this object.
        """
        return int(self.quantize(Decimal('1.00'), ROUND_UP) * 100)

    def to_mongo(self) -> int:
        # b/c
        return self.total_cents

    def __str__(self):
        s = str(self.total_cents).rjust(3, '0')
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
    """ Field for work with money.
    """
    def get_fake(self, document, faker, depth):
        return Money(faker.pydecimal(
            left_digits=5, right_digits=2, positive=True))

    @pass_null
    def prepare_value(self, document, value):
        """ Cast value to :class:`decimal.Decimal`.
        """
        if isinstance(value, Money):
            return value

        elif isinstance(value, (str, Decimal)):
            return Money(value)

        else:
            raise TypeError(repr(value))

    @pass_null
    def to_mongo(self, document, value):
        if isinstance(value, (str, Decimal)):
            return Money(value).total_cents

        elif isinstance(value, Money):
            return value.total_cents

        elif isinstance(value, int):
            return value

        else:
            raise TypeError(repr(value))

    def from_mongo(self, document, data):
        if isinstance(data, int):
            return Money.from_cents(data)
        elif data is AttributeNotSet:
            return AttributeNotSet
        else:
            return self.prepare_value(document, data)
