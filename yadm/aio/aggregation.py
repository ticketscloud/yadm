from yadm.aggregation import BaseAggregator


class AioAggregator(BaseAggregator):
    async def __aiter__(self):
        return self._cursor
