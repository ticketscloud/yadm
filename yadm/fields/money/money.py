""" Field for money.

Use :class:`yadm.fields.money.Money`, as value for money

.. code block: python

    class DocClass(Document):
        money = MoneyField()

    doc = DocClass()
    doc.money = Money('3.14', 'USD')

    db.insert(doc)

This code save to MongoDB document:

.. code block: javascript

    {
        id: ObjectId('534272984c78591787e1a964'),
        money: {v: 314, c: 'USD'}
    }
"""
from decimal import Decimal, Context, ROUND_UP
from functools import wraps
import random

from yadm.fields.base import Field, DefaultMixin, pass_null
from .currency import DEFAULT_CURRENCY_STORAGE
from yadm.markers import AttributeNotSet


def _checker(method):
    """ Decorator for check type of value.
    """
    @wraps(method)
    def wrapper(self, value):
        if not isinstance(value, Money):
            return NotImplemented

        if self.currency != value.currency:
            raise ValueError("Money {!r} and {!r} must be with the"
                             " same currencies".format(str(self), str(value)))
        return method(self, value)

    return wrapper


def _special_comparison(method):
    """ Decorator for special compression Money with other values.
    """
    @wraps(method)
    def wrapper(self, value):
        if isinstance(value, int):
            # This need to compare Money with 0 only integer
            if value:
                return NotImplemented
            else:
                return method(self.value, value)

        elif isinstance(value, Money):
            if self.currency != value.currency:
                raise ValueError("Money {!r} and {!r} must be with"
                                 " the same currencies".format(self, value))
            else:
                return method(self.value, value.value)

        else:
            return NotImplemented

    return wrapper


class Money:
    _context = Context(rounding=ROUND_UP)

    def __init__(self, value, currency=None):
        if isinstance(value, Money):
            if currency is not None:
                raise TypeError("new Money from another Money not need"
                                " currency {!r} as parameter".format(currency))
            else:
                self._value = value.value
                self._currency = value.currency

        else:
            if currency is None:
                raise TypeError("Curency not set.")
            else:
                self._currency = currency = DEFAULT_CURRENCY_STORAGE[currency]

            print(Context(rounding=ROUND_UP))
            print(self._context)

            if isinstance(value, Decimal):
                precision_decimal = Decimal('1.' + '0' * currency.precision)
                self._value = value.quantize(precision_decimal, self._context.rounding)

            else:
                value = Decimal(value, context=self._context)
                precision_decimal = Decimal('1.' + '0' * currency.precision)
                self._value = value.quantize(precision_decimal, self._context.rounding)

    @classmethod
    def from_cents(cls, cents: int, currency) -> 'Money':
        """ Return new Money object from cents.
        """
        _currency = DEFAULT_CURRENCY_STORAGE[currency]
        delimiter = Decimal(10 ** _currency.precision)
        return cls(cents / delimiter, _currency)

    @classmethod
    def from_string(cls, s):
        v, c = s.strip().split()

        if c.isdigit():
            c = int(c)
        else:
            ValueError(s)

        try:
            return cls(v, DEFAULT_CURRENCY_STORAGE[c])
        except KeyError as exc:
            raise ValueError from exc

    @property
    def value(self) -> Decimal:
        return self._value

    @property
    def currency(self):
        return self._currency

    @property
    def total_cents(self) -> int:
        """ Return total cents in this object.
        """
        precision = self.currency.precision
        precision_decimal = Decimal('1.' + '0' * precision)
        quantized = self.value.quantize(precision_decimal, self._context.rounding)
        return int(quantized * 10 ** precision)

    def __abs__(self):
        return self.__class__(abs(self._value), self.currency)

    @_checker
    def __add__(self, target):
        return self.__class__(self.value + target.value, self.currency)

    @_checker
    def __sub__(self, target):
        return self.__class__(self.value - target.value, self.currency)

    def __neg__(self):
        return Money(-self.value, self.currency)

    def __mul__(self, target):
        if isinstance(target, (int, Decimal)):
            return self.__class__(self.value * target, self.currency)
        else:
            return NotImplemented

    def __rmul__(self, target):
        return self.__mul__(target)

    def __truediv__(self, target):
        if isinstance(target, self.__class__):
            if self.currency == target.currency:
                return self.value / target.value
            else:
                raise ValueError("Money {!r} and {!r} must be with"
                                 " the same currencies".format(self, target))
        else:
            return self.__class__(self.value / target, self.currency)

    def __eq__(self, target):
        if target is 0:
            return self.value == target
        else:
            return (isinstance(target, self.__class__) and
                    self.value == target.value and
                    self.currency == target.currency)

    @_special_comparison
    def __gt__(self, other):
        return self > other

    @_special_comparison
    def __lt__(self, other):
        return self < other

    @_special_comparison
    def __ge__(self, other):
        return self > other or self == other

    @_special_comparison
    def __le__(self, other):
        return self < other or self == other

    def __bool__(self):
        return bool(self.value)

    def __hash__(self):
        return hash(str(self))

    def __str__(self):
        precision = self.currency.precision
        precision_decimal = Decimal('1.' + '0' * precision)
        quantized = self.value.quantize(precision_decimal, self._context.rounding)
        return '{} {}'.format(quantized, self._currency.string)

    def __repr__(self):
        return 'Money({!s})'.format(self)

    def to_mongo(self):
        # b/c
        return MoneyField.to_mongo(None, None, self)

    def is_positive(self):
        return self.value > 0

    def is_negative(self):
        return self.value < 0

    def is_zero(self):
        return self.value == 0


class MoneyField(DefaultMixin, Field):
    """ Field to storage money values.
    """
    def __init__(self, bcc=None, **kwargs):
        """ Field for save Money.

        bcc - str,int or Currency value to backward compatibility.
        """
        super().__init__(**kwargs)
        self._bcc = bcc

    def get_fake(self, document, faker, depth):
        value = faker.pydecimal(left_digits=5, right_digits=2, positive=True)
        currency = random.choice(list(DEFAULT_CURRENCY_STORAGE.values()))
        return Money(value, currency)

    @pass_null
    def prepare_value(self, document, value):
        if isinstance(value, Money):
            return value
        else:
            raise TypeError("Only money is allowed for asigment to MoneyField.")

    @pass_null
    def to_mongo(self, document, value):
        return [value.total_cents, value.currency.code]

    @pass_null
    def from_mongo(self, document, data):
        if data is AttributeNotSet:
            return AttributeNotSet

        if isinstance(data, int):
            if not self._bcc:
                raise ValueError("Backward compatibility currency not set in MoneyField.")
            else:
                currency_key = self._bcc
                value = data

        elif isinstance(data, list):
            value, currency_key = data

        elif isinstance(data, dict):  # b/c for version 1.3.0
            currency_key = data['c']
            value = data['v']

        else:
            raise TypeError("Incorrect type of value {!r} for Money".format(type(data)))

        currency = DEFAULT_CURRENCY_STORAGE[currency_key]
        new_value = Decimal(value) / (10 ** currency.precision)

        return Money(new_value, currency)
