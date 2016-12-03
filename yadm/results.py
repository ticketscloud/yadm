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


class UpdateResult(Result):
    def __int__(self):
        return self.modified

    def __bool__(self):
        return self.ok

    @property
    def ok(self):
        return bool(self.raw['ok'])

    @property
    def matched(self):
        return self.raw['n']

    @property
    def upserted(self):
        return self.raw.get('nUpserted', 0)

    @property
    def modified(self):
        return self.raw.get('nModified', 0)


class RemoveResult(Result):
    def __int__(self):
        return self.removed

    def __bool__(self):
        return self.ok

    @property
    def ok(self):
        return bool(self.raw['ok'])

    @property
    def removed(self):
        return self.raw['n']
