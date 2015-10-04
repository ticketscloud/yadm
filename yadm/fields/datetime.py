from datetime import datetime, date

import pytz
import dateutil.parser

from yadm.fields.base import Field, DefaultMixin


class DatetimeField(DefaultMixin, Field):
    """ Field for time stamp

    :param bool auto_now: datetime.now as default
        (default: False)
    """
    def __init__(self, auto_now=False, **kwargs):
        self.auto_now = auto_now
        super().__init__(**kwargs)

    @staticmethod
    def _fix_timezone(value):
        if value.tzinfo is None:
            return value.replace(tzinfo=pytz.utc)
        else:
            return value

    def get_default(self, document):
        if self.auto_now:
            return datetime.now(pytz.utc)
        else:
            return super().default

    def get_fake(self, document, faker, depth):
        if self.auto_now:
            return datetime.now(pytz.utc)
        else:
            return faker.date_time()

    @classmethod
    def prepare_value(cls, document, value):
        if value is None:
            return None

        if isinstance(value, datetime):
            return cls._fix_timezone(value)

        elif isinstance(value, date):
            return datetime(
                *value.timetuple()[:3],
                tz=pytz.utc
            )

        elif isinstance(value, str):
            value = dateutil.parser.parse(value)
            return cls._fix_timezone(value)

        else:
            raise TypeError('First value must be datetime,'
                            ' date or string, but {!r}'.format(type(value)))

    @classmethod
    def from_mongo(cls, document, value):
        if value is not None:
            return cls._fix_timezone(value)

    @classmethod
    def to_mongo(cls, document, value):
        if value is not None:
            return cls._fix_timezone(value)
