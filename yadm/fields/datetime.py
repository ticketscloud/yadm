import re
import datetime

from yadm.fields.base import Field


RE_datetime_ISO = re.compile(
    r'^(\d\d\d\d)-(\d\d)-(\d\d)[\sTt](\d\d):(\d\d)(?::(\d\d))?(?:\.(\d+))?$')
RE_date_ISO = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)$')
RE_time_ISO = re.compile(r'^(\d\d):(\d\d)(?::(\d\d))?(?:\.(\d+))?$')


def parse_ISO(string):
    """ It returns `datetime.date`, `datetime.time`
    or `datetime.datetime` from date in ISO format
    """
    r = RE_datetime_ISO.match(string)
    if r:
        tt = [int(i) for i in r.groups(0)]
        tt[-1] = tt[-1] * (10 ** (6 - len(str(tt[-1]))))
        return datetime.datetime(*tt)

    r = RE_date_ISO.match(string)
    if r:
        tt = [int(i) for i in r.groups(0)]
        return datetime.date(*tt)

    r = RE_time_ISO.match(string)
    if r:
        tt = [int(i) for i in r.groups(0)]
        tt[-1] = tt[-1] * (10 ** (6 - len(str(tt[-1]))))
        return datetime.time(*tt)

    raise ValueError('The bad ISO datetime string: %r' % string)


class DatetimeField(Field):
    """ Field for time stamp

    :param bool auto_now: datetime.now as default
        (default: False)
    """
    def __init__(self, auto_now=False, **kwargs):
        self.auto_now = auto_now
        super().__init__(**kwargs)

    @property
    def default(self):
        if self.auto_now:
            return datetime.datetime.now()
        else:
            return super().default

    @staticmethod
    def prepare_value(dt):
        if type(dt) is datetime.datetime:
            return dt

        elif type(dt) is datetime.date:
            return datetime.datetime(*dt.timetuple()[:3])

        elif isinstance(dt, str):
            dt = parse_ISO(dt)

            if type(dt) is datetime.datetime:
                return dt

            elif type(dt) is datetime.date:
                return datetime.datetime(*dt.timetuple()[:3])

            else:
                raise ValueError('Bad ISO date or datetime: {!r}'.format(dt))

        else:
            raise TypeError('First value must be datetime,'
                            ' date or string, but {!r}'.format(type(dt)))
