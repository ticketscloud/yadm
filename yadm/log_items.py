from typing import NamedTuple, Optional, Any, Dict

from bson import ObjectId


class BaseLog:  # pragma: no cover
    def __init__(self):
        self.items = []

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index):
        return self.items[index]

    def __contains__(self, item):
        return item in self.items

    def __len__(self):
        return len(self.items)

    def __bool__(self):
        return bool(self.items)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self.items))

    def append(self, log_item):
        self.items.append(log_item)

    def clear(self):
        self.items.clear()


class Save(NamedTuple):
    op: str = 'save'
    id: Optional[ObjectId] = None


class Insert(NamedTuple):
    op: str = 'insert'
    id: Optional[ObjectId] = None


class UpdateOne(NamedTuple):
    update_data: Dict[str, Any]
    op: str = 'update_one'


class DeleteOne(NamedTuple):
    op: str = 'delete_one'


class Reload(NamedTuple):
    op: str = 'reload'


class SetField(NamedTuple):
    name: str
    value: Any
    op: str = 'set_field'


class ChangeChild(NamedTuple):
    path: str
    name: str
    log_item: NamedTuple
    op: str = 'change_chind'
