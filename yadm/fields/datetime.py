import datetime

import dateutil.parser

from yadm.fields.base import Field


class DatetimeField(Field):
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
            return value.replace(tzinfo=datetime.timezone.utc)
        else:
            return value

    @property
    def default(self):
        if self.auto_now:
            return datetime.datetime.now(datetime.timezone.utc)
        else:
            return super().default

    @classmethod
    def prepare_value(cls, document, value):
        if value is None:
            return None

        if type(value) is datetime.datetime:
            return cls._fix_timezone(value)

        elif type(value) is datetime.date:
            return datetime.datetime(
                *value.timetuple()[:3],
                tz=datetime.timezone.utc
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
