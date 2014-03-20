from structures import Structure

from yadm.fields import ObjectIdField


class BaseDocument(Structure):
    __collection__ = None


class Document(BaseDocument):
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
