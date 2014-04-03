import datetime

import structures.fields_datetime

from yadm.fields.base import DatabaseFieldMixin


class DatetimeField(DatabaseFieldMixin, structures.fields_datetime.DateTime):
    """ Field for time stamp

    :param bool auto_now: datetime.now as default
        (default: False)
    """
    def __init__(self, auto_now=False):
        super().__init__()
        self.auto_now = auto_now

    @property
    def default(self):
        if self.auto_now:
            now = datetime.datetime.now()
            return now
        else:
            return super().default

    @default.setter
    def default(self, value):
        """ For work `super().__init__()`
        """
