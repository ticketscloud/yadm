from .simple import StringField


class EmailField(StringField):
    def get_fake(self, document, faker, depth):
        return faker.email().lower()

    def prepare_value(self, document, value):
        value = super().prepare_value(document, value)

        if value is not None:
            if '@' not in value:
                raise ValueError('"{}" is not email'.format(value))

            return value.lower()
