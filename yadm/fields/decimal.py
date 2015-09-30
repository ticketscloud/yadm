"""
Field for decimal numbers

.. code block: python

    class DocClass(Document):
        dec = DecimalField()

    doc = DocClass()
    doc.dec = Decimal('3.14')

    db.insert(doc)

This code save to MongoDB document:

.. code block: javascript

    {
        id: ObjectId('534272984c78591787e1a964'),
        dec: {i: 314, e: -2}
    }

"""
from decimal import Decimal, getcontext
from functools import reduce

from yadm.fields.base import Field, DefaultMixin
from yadm.markers import AttributeNotSet


class DecimalField(DefaultMixin, Field):
    """ Field for work with :class:`decimal.Decimal`

    :param decimal.Context context: context for decimal operations
        (default: run :func:`decimal.getcontext` when need)
    :param decimal.Decimal default:
    """
    def __init__(self, context=None, default=AttributeNotSet):
        self.context = context
        super().__init__(default=default)

    @property
    def context(self):
        """ Context

        :return: :class:`decimal.Context` for values
        """
        return self._context if self._context else getcontext()

    @context.setter
    def context(self, value):
        self._context = value

    def get_fake(self, document, faker, depth):
        return faker.pydecimal()

    @staticmethod
    def _integer_from_digits(digits):
        """ Make integer from digits

        :param list digits: list of digits as integers
        :return: result integer

        [2, 4, 6, 7] => 2467
        """
        return reduce(lambda cur, acc: cur * 10 + acc, digits, 0)

    def prepare_value(self, document, value):
        """ Cast value to :class:`decimal.Decimal`
        """
        if value is None:
            return None
        elif isinstance(value, Decimal):
            return value
        elif isinstance(value, (str, int)):
            return Decimal(value, context=self.context)
        else:
            raise TypeError(value)

    def to_mongo(self, document, value):
        if value is None:
            return None
        else:
            sign, digits, exp = value.as_tuple()
            integer = self._integer_from_digits(digits)
            return {
                'i': -integer if sign else integer,
                'e': exp
            }

    def from_mongo(self, document, value):
        if value is AttributeNotSet:
            raise AttributeError(self.name)

        sign = value['i'] < 0  # False - positive, True - negative

        digits = []
        i = abs(value['i'])

        while i:
            i, d = divmod(i, 10)
            digits.append(d)

        digits.reverse()

        return Decimal((sign, digits, value['e']), context=self.context)
