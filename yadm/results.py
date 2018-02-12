from collections.abc import Mapping


class Result(Mapping):
    def __init__(self, raw):
        self._raw = raw

    @property
    def raw(self):
        return self._raw

    def __getitem__(self, key):
        return self.raw[key]

    def __iter__(self):
        return iter(self.raw)

    def __len__(self):
        return len(self.raw)
