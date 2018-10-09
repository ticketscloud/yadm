from yadm.aggregation import BaseAggregator


class AioAggregator(BaseAggregator):
    async def __aiter__(self):
        async for item in self._cursor:
            yield item
