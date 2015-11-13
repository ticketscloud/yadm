===========
Base fields
===========

.. automodule:: yadm.fields.base

.. autoclass:: FieldDescriptor
    :members:

.. autoclass:: Field
    :members:


Smart null
----------

.. _smart_null:

Base :ref:`Field` accept keyword argument `smart_null`.
If it `True`, access to not exist fields return `None` instead `AttributeError` exception.
You will not be able to distinguish null key from not exist. Use with care.
