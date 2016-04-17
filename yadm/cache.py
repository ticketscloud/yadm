import abc


class CacheInterface(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __getitem__(self, key):  # noqa
        pass

    @abc.abstractmethod
    def __setitem__(self, key, value):  # noqa
        pass

    @abc.abstractmethod
    def __delitem__(self, key):  # noqa
        pass

    @abc.abstractmethod
    def __contains__(self, key):  # noqa
        pass

    @abc.abstractmethod
    def __len__(self, key):  # noqa
        pass


@CacheInterface.register
class NotACache:
    def __getitem__(self, key):  # noqa
        raise KeyError(key)

    def __setitem__(self, key, value):  # noqa
        pass

    def __delitem__(self, key):  # noqa
        pass

    def __contains__(self, key):  # noqa
        return False

    def __len__(self, key):  # noqa
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
