""" Field for decimal numbers.
"""
from decimal import Decimal, getcontext, Context
from functools import reduce
from typing import Union, Optional, Iterable


from bson import Decimal128

from yadm.documents import BaseDocument
from yadm.fields.base import Field, DefaultMixin, pass_null


TDecimalable = Union[Decimal, Decimal128, str, int]
TDecimalInMongo = Union[Decimal, Decimal128, dict]
TDecimal128able = Union[Decimal, Decimal128, str]


class DecimalField(DefaultMixin, Field):
    """ Field for work with :class:`decimal.Decimal`.

    TODO: context in copy()
    """
    _context = None

    def __init__(self, *, context: Optional[Context] = None, **kwargs: dict):
        super().__init__(**kwargs)
        self.context = context

    @property
    def context(self) -> Context:
        """ Context.

        :return: :class:`decimal.Context` for values
        """
        return self._context if self._context else getcontext()

    @context.setter
    def context(self, context: Optional[Context]):
        self._context = context

    def get_fake(self, document: BaseDocument, faker, depth):  # pragma: no cover
        return faker.pydecimal()

    @staticmethod
    def _integer_from_digits(digits: Iterable[int]) -> int:
        """ Make integer from digits.

        [2, 4, 6, 7] => 2467
        """
        return reduce(lambda cur, acc: cur * 10 + acc, digits, 0)

    @pass_null
    def prepare_value(self, document: BaseDocument,
                      value: TDecimalable) -> Decimal:
        """ Cast value to :class:`decimal.Decimal`.
        """
        if isinstance(value, Decimal):
            return value
        elif isinstance(value, (str, int)):
            return Decimal(value, context=self.context)
        else:  # pragma: no cover
            raise TypeError(value)

    @pass_null
    def to_mongo(self, document: BaseDocument,
                 value: Decimal) -> dict:
        sign, digits, exp = value.as_tuple()
        integer = self._integer_from_digits(digits)
        return {
            'i': -integer if sign else integer,
            'e': exp
        }

    @pass_null
    def from_mongo(self, document: BaseDocument,
                   value: TDecimalInMongo) -> Decimal:
        if isinstance(value, dict):
            sign = value['i'] < 0  # False - positive, True - negative

            digits = []
            i = abs(value['i'])

            while i:
                i, d = divmod(i, 10)
                digits.append(d)

            digits.reverse()

            return Decimal((sign, digits, value['e']), context=self.context)

        elif isinstance(value, Decimal128):
            return value.to_decimal()

        elif isinstance(value, Decimal):
            return value

        else:  # pragma: no cover
            raise TypeError(value)


class Decimal128Field(DefaultMixin, Field):
    @pass_null
    def prepare_value(self, document: BaseDocument,
                      value: TDecimal128able) -> Decimal:
        if isinstance(value, Decimal128):
            return value
        elif isinstance(value, (str, Decimal)):
            return Decimal128(value)
        else:  # pragma: no cover
            raise TypeError(value)
