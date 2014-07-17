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
    def _fix_timezone(dt):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=datetime.timezone.utc)
        else:
            return dt

    @property
    def default(self):
        if self.auto_now:
            return datetime.datetime.now(datetime.timezone.utc)
        else:
            return super().default

    @classmethod
    def prepare_value(cls, dt):
        if type(dt) is datetime.datetime:
            return cls._fix_timezone(dt)

        elif type(dt) is datetime.date:
            dt = datetime.datetime(*dt.timetuple()[:3])
            return cls._fix_timezone(dt)

        elif isinstance(dt, str):
            dt = dateutil.parser.parse(dt)
            return cls._fix_timezone(dt)

        else:
            raise TypeError('First value must be datetime,'
                            ' date or string, but {!r}'.format(type(dt)))
