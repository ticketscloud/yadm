from structures import Structure

from yadm.fields import ObjectIdField


class BaseDocument(Structure):
    def __str__(self):
        return self(id(self))

    def __repr__(self):
        return '{}({})'.format(self.__class__.name, str(self))


class Document(BaseDocument):
    __collection__ = None
    __db__ = None

    _id = ObjectIdField

    def __str__(self):
        return 'collection: "{}"'.format(self.__collection__)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @id.deleter
    def id(self, id):
        del self._id
