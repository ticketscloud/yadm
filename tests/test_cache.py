from yadm.cache import StackCache


def test_stack_getsetcontains():
    cache = StackCache(2)
    assert len(cache) == 0

    cache['a'] = 1
    cache['b'] = 2

    assert len(cache) == 2
    assert cache == {'a': 1, 'b': 2}
    assert cache['a'] == 1
    assert 'a' in cache
    assert 'c' not in cache


def test_stack_owerflow():
    cache = StackCache(2)
    assert len(cache) == 0

    cache['a'] = 1
    cache['b'] = 2
    cache['c'] = 3

    assert len(cache) == 2
    assert cache == {'b': 2, 'c': 3}
    assert 'a' not in cache
    assert 'c' in cache
