"""
Mongo Aggregation Framework helper.

.. code-block:: python

    cur = db.aggregate(Doc).match({'i': {'$gt': 13}}).project(a='$i').limit(8)
"""


class BaseAggregator:
    def __init__(self, db, document_class, *,
                 pipeline=None, collection_params=None):
        self._db = db
        self._document_class = document_class
        self._pipeline = [] if pipeline is None else pipeline
        self._collection_params = collection_params

    def __repr__(self):
        return ("{s.__class__.__name__}("
                "{s._document_class.__collection__} {s._pipeline})"
                "".format(s=self))

    def __getattr__(self, op):
        return AgOperator(self, op)

    @property
    def _cursor(self):  # noqa
        """ pymongo aggregate cursor.
        """
        collection = self._db._get_collection(self._document_class)
        return collection.aggregate(self._pipeline)


class Aggregator(BaseAggregator):
    def __iter__(self):
        return iter(self._cursor)

    def __getitem__(self, index):
        if isinstance(index, int):
            if index > 0:
                try:
                    return self.skip(index).limit(1)[0]
                except IndexError:
                    raise IndexError("index out of range: {}".format(index))

            elif index == 0:
                try:
                    return next(iter(self))
                except StopIteration:
                    raise IndexError("index out of range: 0")

            else:
                raise NotImplementedError("negative indexed are not implemented")

        elif isinstance(index, slice):
            if index.step is None or index.step == 1:
                start = index.start
                stop = index.stop
                return self.skip(start).limit(stop - start)
            else:
                raise NotImplementedError("stepped slice is not implemented")

        else:
            raise TypeError("{s.__class__.__name__}"
                            " indices must be integers,"
                            " not {t.__name__}"
                            "".format(s=self, t=type(index)))


class AgOperator:
    def __init__(self, aggregate, op):
        self._aggregate = aggregate
        self._op = op if op.startswith('$') else '${}'.format(op)

    def __call__(self, _value=None, **kwargs):
        if _value is not None and kwargs:
            raise ValueError()

        value = kwargs if kwargs else _value
        if not value:
            raise ValueError("empty value")

        pipeline = self._aggregate._pipeline.copy()
        pipeline.append({self._op: value})
        return self._aggregate.__class__(self._aggregate._db,
                                         self._aggregate._document_class,
                                         pipeline=pipeline)

    def __repr__(self):
        return ("{s.__class__.__name__}"
                "({a._document_class.__collection__} {a._pipeline} {s._op})"
                "".format(s=self, a=self._aggregate))
