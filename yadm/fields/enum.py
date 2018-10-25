from yadm.fields.base import pass_null
from yadm.fields.simple import SimpleField
from yadm.markers import AttributeNotSet


class EnumField(SimpleField):
    """ Field for enum.Enum .
    """
    def __init__(self, enum, **kwargs):
        self.type = enum
        super().__init__(**kwargs)

    def copy(self):
        return self.__class__(self.type,
                              smart_null=self.smart_null,
                              default=self.default)

    @pass_null
    def to_mongo(self, document, value):
        return value.value


class EnumStateSetError(Exception):
    def __init__(self, current, new):
        self.current = current
        self.new = new
        self.message = ("Not allowed in rules: {} -> {}"
                        "".format(current, new))

        super().__init__(self.message)


class EnumStateField(EnumField):
    """ Simple state machine with states are enum.Enum .
    """
    rules = None

    def __init__(self, enum, rules=None, start=AttributeNotSet, **kwargs):
        super().__init__(enum, default=start, **kwargs)

        if rules is not None:
            self.rules = rules

        if not self.rules:  # pragma: no cover
            raise ValueError("Rules list is empty")

    def copy(self):
        return self.__class__(self.type, self.rules,
                              smart_null=self.smart_null,
                              default=self.default)

    @pass_null
    def prepare_value(self, document, value):
        current_value = getattr(document, self.name, AttributeNotSet)
        new_value = super().prepare_value(document, value)

        if (new_value != current_value and
                new_value not in self.rules.get(current_value, [])):
            raise EnumStateSetError(current_value, new_value)

        return new_value
