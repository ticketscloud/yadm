=========
Documents
=========

.. automodule:: yadm.documents


.. autoclass:: BaseDocument(**kwargs)
    :members: __str__


.. autoclass:: Document(**kwargs)
    :members:

    .. py:attribute:: __collection__

        Name of MongoDB collection

    .. py:attribute:: _id

        Mongo object id (:py:class:`bson.ObjectId`)

    .. py:attribute:: id

        Alias for :py:attr:`_id` for simply use

    .. py:attribute:: __db__

        Internal attribute contain instance of :py:class:`yadm.database.Database`
        for realize :py:class:`yadm.fields.references.ReferenceField`.
        It bind in :py:class:`yadm.database.Database` or :py:class:`yadm.queryset.QuerySet`.

.. autoclass:: EmbeddedDocument(**kwargs)
    :members:
