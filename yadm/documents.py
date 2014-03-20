from structures import Structure

from yadm.fields import ObjectIdField


class BaseDocument(Structure):
    pass


class Document(BaseDocument):
    __collection__ = None
    __db__ = None

    _id = ObjectIdField

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @id.deleter
    def id(self, id):
        del self._id
