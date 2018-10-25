from yadm.log_items import BaseLog, ChangeChild


class ItemLog(BaseLog):
    def __init__(self, document_item):
        super().__init__()
        self.document_item = document_item

    def append(self, log_item):
        self.items.append(log_item)

        root = self.document_item.__document__
        if root is not None:
            self.document_item.__document__.__log__.append(
                ChangeChild(
                    path=self.document_item.__field_name__,
                    name=self.document_item.__name__,
                    log_item=log_item,
                ),
            )


class DocumentItemMixin:
    """ Mixin for custom all fields values, such as EmbeddedDocument,
        yadm.fields.containers.Container.
    """
    __parent__ = None
    __name__ = None
    __qs__ = None
    __log__ = None

    def __init__(self, *args, **kwargs):
        self.__log__ = ItemLog(self)
        super().__init__(*args, **kwargs)

    @property
    def __document__(self):
        """ Root document.

        .. code-block:: python

                assert doc.f.l[0].__document__ is doc
        """
        obj = self

        while getattr(obj, '__parent__', None) is not None:
            obj = obj.__parent__

        if obj is not self:
            return obj
        else:
            return None

    @property
    def __db__(self):
        """ Database object.

        .. code-block:: python

            assert doc.f.l[0].__db__ is doc.__db__
        """
        document = self.__document__
        if document is not None:
            return document.__db__
        else:
            return None  # pragma: no cover

    @property
    def __qs__(self):
        """ Queryset object.
        """
        document = self.__document__
        if document is not None:
            return document.__qs__
        else:
            return None  # pragma: no cover

    @property
    def __path__(self):
        """ Path to root generator.

        .. code-block:: python

            assert list(doc.f.l[0].__path__) == [doc.f.l[0], doc.f.l, doc.f]
        """
        obj = self

        while getattr(obj, '__parent__', None) is not None:
            yield obj
            obj = obj.__parent__

    @property
    def __path_names__(self):
        """ Path to root generator.

        .. code-block:: python

            assert list(doc.f.l[0].__path__) == [0, 'l', 'f']
        """
        for item in self.__path__:
            yield item.__name__

    @property
    def __field_name__(self):
        """ Dotted field name for MongoDB opperations, like as $set, $push and other...

        .. code-block:: python

            assert doc.f.l[0].__field_name__ == 'f.l.0'
        """
        return '.'.join(reversed([str(i) for i in self.__path_names__]))

    def __get_value__(self, document):
        """ Get value from document with path to self.
        """
        obj = document

        for name in reversed(list(self.__path_names__)):
            if isinstance(name, int):
                obj = obj[name]
            else:
                obj = getattr(obj, name)

        return obj
