""" Currencies for Money.
"""
from collections import namedtuple
import random

from yadm.markers import AttributeNotSet
from yadm.fields.base import Field, DefaultMixin, pass_null


Currency = namedtuple('Currency', ['code', 'string', 'precision'])

DEFAULT_CURRENCIES_LIST = [
    Currency(784, 'AED', 2),
    Currency(971, 'AFN', 2),
    Currency(8, 'ALL', 2),
    Currency(51, 'AMD', 2),
    Currency(532, 'ANG', 2),
    Currency(973, 'AOA', 2),
    Currency(32, 'ARS', 2),
    Currency(36, 'AUD', 2),
    Currency(533, 'AWG', 2),
    Currency(944, 'AZN', 2),
    Currency(977, 'BAM', 2),
    Currency(52, 'BBD', 2),
    Currency(50, 'BDT', 2),
    Currency(975, 'BGN', 2),
    Currency(48, 'BHD', 3),
    Currency(108, 'BIF', 0),
    Currency(60, 'BMD', 2),
    Currency(96, 'BND', 2),
    Currency(68, 'BOB', 2),
    Currency(984, 'BOV', 2),
    Currency(986, 'BRL', 2),
    Currency(44, 'BSD', 2),
    Currency(64, 'BTN', 2),
    Currency(72, 'BWP', 2),
    Currency(974, 'BYR', 0),
    Currency(84, 'BZD', 2),
    Currency(124, 'CAD', 2),
    Currency(976, 'CDF', 2),
    Currency(947, 'CHE', 2),
    Currency(756, 'CHF', 2),
    Currency(948, 'CHW', 2),
    Currency(990, 'CLF', 4),
    Currency(152, 'CLP', 0),
    Currency(156, 'CNY', 2),
    Currency(170, 'COP', 2),
    Currency(970, 'COU', 2),
    Currency(188, 'CRC', 2),
    Currency(931, 'CUC', 2),
    Currency(192, 'CUP', 2),
    Currency(132, 'CVE', 0),
    Currency(203, 'CZK', 2),
    Currency(262, 'DJF', 0),
    Currency(208, 'DKK', 2),
    Currency(214, 'DOP', 2),
    Currency(12, 'DZD', 2),
    Currency(818, 'EGP', 2),
    Currency(232, 'ERN', 2),
    Currency(230, 'ETB', 2),
    Currency(978, 'EUR', 2),
    Currency(242, 'FJD', 2),
    Currency(238, 'FKP', 2),
    Currency(826, 'GBP', 2),
    Currency(981, 'GEL', 2),
    Currency(936, 'GHS', 2),
    Currency(292, 'GIP', 2),
    Currency(270, 'GMD', 2),
    Currency(324, 'GNF', 0),
    Currency(320, 'GTQ', 2),
    Currency(328, 'GYD', 2),
    Currency(344, 'HKD', 2),
    Currency(340, 'HNL', 2),
    Currency(191, 'HRK', 2),
    Currency(332, 'HTG', 2),
    Currency(348, 'HUF', 2),
    Currency(360, 'IDR', 2),
    Currency(376, 'ILS', 2),
    Currency(356, 'INR', 2),
    Currency(368, 'IQD', 3),
    Currency(364, 'IRR', 2),
    Currency(352, 'ISK', 0),
    Currency(388, 'JMD', 2),
    Currency(400, 'JOD', 3),
    Currency(392, 'JPY', 0),
    Currency(404, 'KES', 2),
    Currency(417, 'KGS', 2),
    Currency(116, 'KHR', 2),
    Currency(174, 'KMF', 0),
    Currency(408, 'KPW', 2),
    Currency(410, 'KRW', 0),
    Currency(414, 'KWD', 3),
    Currency(136, 'KYD', 2),
    Currency(398, 'KZT', 2),
    Currency(418, 'LAK', 2),
    Currency(422, 'LBP', 2),
    Currency(144, 'LKR', 2),
    Currency(430, 'LRD', 2),
    Currency(426, 'LSL', 2),
    Currency(434, 'LYD', 3),
    Currency(504, 'MAD', 2),
    Currency(498, 'MDL', 2),
    Currency(969, 'MGA', 1),
    Currency(807, 'MKD', 2),
    Currency(104, 'MMK', 2),
    Currency(496, 'MNT', 2),
    Currency(446, 'MOP', 2),
    Currency(478, 'MRO', 1),
    Currency(480, 'MUR', 2),
    Currency(462, 'MVR', 2),
    Currency(454, 'MWK', 2),
    Currency(484, 'MXN', 2),
    Currency(979, 'MXV', 2),
    Currency(458, 'MYR', 2),
    Currency(943, 'MZN', 2),
    Currency(516, 'NAD', 2),
    Currency(566, 'NGN', 2),
    Currency(558, 'NIO', 2),
    Currency(578, 'NOK', 2),
    Currency(524, 'NPR', 2),
    Currency(554, 'NZD', 2),
    Currency(512, 'OMR', 3),
    Currency(590, 'PAB', 2),
    Currency(604, 'PEN', 2),
    Currency(598, 'PGK', 2),
    Currency(608, 'PHP', 2),
    Currency(586, 'PKR', 2),
    Currency(985, 'PLN', 2),
    Currency(600, 'PYG', 0),
    Currency(634, 'QAR', 2),
    Currency(946, 'RON', 2),
    Currency(941, 'RSD', 2),
    Currency(643, 'RUB', 2),
    Currency(646, 'RWF', 0),
    Currency(682, 'SAR', 2),
    Currency(90, 'SBD', 2),
    Currency(690, 'SCR', 2),
    Currency(938, 'SDG', 2),
    Currency(752, 'SEK', 2),
    Currency(702, 'SGD', 2),
    Currency(654, 'SHP', 2),
    Currency(694, 'SLL', 2),
    Currency(706, 'SOS', 2),
    Currency(968, 'SRD', 2),
    Currency(728, 'SSP', 2),
    Currency(678, 'STD', 2),
    Currency(760, 'SYP', 2),
    Currency(748, 'SZL', 2),
    Currency(764, 'THB', 2),
    Currency(972, 'TJS', 2),
    Currency(934, 'TMT', 2),
    Currency(788, 'TND', 3),
    Currency(776, 'TOP', 2),
    Currency(949, 'TRY', 2),
    Currency(780, 'TTD', 2),
    Currency(901, 'TWD', 2),
    Currency(834, 'TZS', 2),
    Currency(980, 'UAH', 2),
    Currency(800, 'UGX', 0),
    Currency(840, 'USD', 2),
    Currency(997, 'USN', 2),
    Currency(998, 'USS', 2),
    Currency(940, 'UYI', 0),
    Currency(858, 'UYU', 2),
    Currency(860, 'UZS', 2),
    Currency(937, 'VEF', 2),
    Currency(704, 'VND', 0),
    Currency(548, 'VUV', 0),
    Currency(882, 'WST', 2),
    Currency(950, 'XAF', 0),
    Currency(961, 'XAG', 0),
    Currency(959, 'XAU', 0),
    Currency(955, 'XBA', 0),
    Currency(956, 'XBB', 0),
    Currency(957, 'XBC', 0),
    Currency(958, 'XBD', 0),
    Currency(951, 'XCD', 2),
    Currency(960, 'XDR', 0),
    Currency(952, 'XOF', 0),
    Currency(964, 'XPD', 0),
    Currency(953, 'XPF', 0),
    Currency(962, 'XPT', 0),
    Currency(994, 'XSU', 0),
    Currency(963, 'XTS', 0),
    Currency(965, 'XUA', 0),
    Currency(999, 'XXX', 0),
    Currency(886, 'YER', 2),
    Currency(710, 'ZAR', 2),
    Currency(967, 'ZMW', 2),
]


class CurrencyStorage(dict):
    def __init__(self, currencies):
        super().__init__()
        for code, string, precision in currencies:
            currency = Currency(code, string, precision)
            self[code] = currency
            self[string] = currency

    def __getitem__(self, item):
        if isinstance(item, (str, int)):
            if item in self:
                return super().__getitem__(item)
            else:
                raise KeyError("{!r} is invalid value of CurrencyStorage."
                               "".format(item))

        elif isinstance(item, Currency):
            return self[item.code]

        else:  # pragma: no cover
            raise TypeError("{!r} is invalid type value of CurrencyStorage."
                            "".format(type(item)))


DEFAULT_CURRENCY_STORAGE = CurrencyStorage(DEFAULT_CURRENCIES_LIST)


class CurrencyField(DefaultMixin, Field):
    def __init__(self, *,
                 default=AttributeNotSet,
                 currency_storage=DEFAULT_CURRENCY_STORAGE):
        self._currency_storage = currency_storage

        if default is AttributeNotSet:
            pass  # pragma: no cover
        elif isinstance(default, (Currency, str, int)):
            default = self._currency_storage[default]
        else:  # pragma: no cover
            raise TypeError("Bad type for default.")

        super().__init__(default=default)

    def get_fake(self, document, faker, depth):  # pragma: no cover
        return random.choice(list(DEFAULT_CURRENCY_STORAGE.values()))

    @pass_null
    def prepare_value(self, document, value):
        if isinstance(value, Currency):
            return value
        elif isinstance(value, (str, int)):
            if value in self._currency_storage:
                return self._currency_storage[value]
            else:
                raise ValueError("Invalid value for CurrencyField.")
        else:
            raise TypeError("Only Currency or None is allowed for CurrencyField.")

    @pass_null
    def to_mongo(self, document, value):
        return value.string

    @pass_null
    def from_mongo(self, document, value):
        return self._currency_storage[value]
