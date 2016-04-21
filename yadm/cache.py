import abc


class CacheInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __getitem__(self, key):  # pragma: no cover
        pass

    @abc.abstractmethod
    def __setitem__(self, key, value):  # pragma: no cover
        pass

    @abc.abstractmethod
    def __delitem__(self, key):  # pragma: no cover
        pass

    @abc.abstractmethod
    def __contains__(self, key):  # pragma: no cover
        pass

    @abc.abstractmethod
    def __len__(self, key):  # pragma: no cover
        pass


@CacheInterface.register
class NotACache:
    def __getitem__(self, key):  # pragma: no cover
        raise KeyError(key)

    def __setitem__(self, key, value):  # pragma: no cover
        pass

    def __delitem__(self, key):  # pragma: no cover
        pass

    def __contains__(self, key):  # pragma: no cover
        return False

    def __len__(self, key):  # pragma: no cover
        return 0


@CacheInterface.register
class StackCache(dict):
    def __init__(self, size):
        self.size = size
        self._stack = []

    def __setitem__(self, key, item):
        if key not in self:
            self._stack.append(key)

            if len(self._stack) > self.size:
                del self[self._stack[0]]

        super().__setitem__(key, item)

    def __delitem__(self, key):
        super().__delitem__(key)
        self._stack.remove(key)
