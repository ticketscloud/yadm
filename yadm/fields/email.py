from yadm.fields.base import pass_null
from yadm.fields.simple import StringField


class InvalidEmail(ValueError):
    """ Raise if value is not correct email address
    """


class EmailField(StringField):
    def get_fake(self, document, faker, depth):
        return faker.email().lower()

    @pass_null
    def prepare_value(self, document, value):
        value = super().prepare_value(document, value).lower()
        self.check_email(value)
        return value

    @classmethod
    def check_email(cls, value):
        """ Check email classmethod.

        Raise `ValueError` if value is not email.

        Usage: `EmailField.check_email(some_untrusted_email)`
        """
        if '@' not in value:
            cls._raise(value)

        parts = value.split('@')
        if len(parts) != 2 or len([p for p in parts if p]) != 2:
            cls._raise(value)

    @staticmethod
    def _raise(value):
        raise InvalidEmail('"{}" is not email'.format(value))
