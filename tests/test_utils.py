import pickle
import unittest

import pytest

from xcube_smos.utils import LruCache


class LruCacheTest(unittest.TestCase):
    def test_get(self):
        c = LruCache[str, int](max_size=3)
        self.assertEqual(3, c.max_size)
        self.assertEqual(0, c.size)
        self.assertEqual(None, c.get('x'))
        self.assertEqual(0, c.size)
        self.assertEqual(13, c.get('x', 13))
        self.assertEqual(0, c.size)

    def test_put(self):
        c = LruCache[str, int](max_size=3)
        self.assertEqual(3, c.max_size)
        self.assertEqual(0, c.size)

        # Should still fit, LRU is 'x'
        c.put('x', 13)
        self.assertEqual(13, c.get('x'))
        self.assertEqual(1, c.size)
        self.assertEqual(['x'], list(c.keys()))

        # Should still fit, LRU is 'y'
        c.put('y', 58)
        self.assertEqual(58, c.get('y'))
        self.assertEqual(2, c.size)
        self.assertEqual(['y', 'x'], list(c.keys()))

        # Should still fit, LRU is 'z'
        c.put('z', 32)
        self.assertEqual(32, c.get('z'))
        self.assertEqual(3, c.size)
        self.assertEqual(['z', 'y', 'x'], list(c.keys()))

        # Now reach limit 3, LRU is 'u', drop 'x'
        c.put('u', 81)
        self.assertEqual(81, c.get('u'))
        self.assertEqual(3, c.size)
        self.assertEqual(['u', 'z', 'y'], list(c.keys()))

        # make 'y' LRU
        self.assertEqual(58, c.get('y'))
        self.assertEqual(3, c.size)
        self.assertEqual(['y', 'u', 'z'], list(c.keys()))

    def test_clear(self):
        c = LruCache[str, int](max_size=3)

        c.put('x', 13)
        c.put('y', 58)
        c.put('z', 32)
        self.assertEqual(3, c.size)
        self.assertEqual(['z', 'y', 'x'], list(c.keys()))
        self.assertEqual([32, 58, 13], list(c.values()))

        c.clear()
        self.assertEqual(0, c.size)
        self.assertEqual([], list(c.keys()))
        self.assertEqual([], list(c.values()))

    def test_zero_size(self):
        c = LruCache[str, int](max_size=0)
        c.put('x', 13)
        c.put('y', 58)
        self.assertEqual(0, c.size)
        self.assertEqual(None, c.get('x'))
        self.assertEqual(None, c.get('y'))
        self.assertEqual([], list(c.keys()))
        self.assertEqual([], list(c.values()))

    def test_dispose(self):
        disposed_values = []

        def dispose_value(v):
            nonlocal disposed_values
            disposed_values.append(v)

        c = LruCache[str, int](max_size=3, dispose_value=dispose_value)

        c.put('x', 13)
        c.put('y', 58)
        c.put('z', 32)
        self.assertEqual(['z', 'y', 'x'], list(c.keys()))
        self.assertEqual([32, 58, 13], list(c.values()))
        self.assertEqual([], disposed_values)

        c.put('y', 59)
        self.assertEqual(['y', 'z', 'x'], list(c.keys()))
        self.assertEqual([59, 32, 13], list(c.values()))
        self.assertEqual([58], disposed_values)

        disposed_values = []
        c.put('u', 81)
        self.assertEqual(['u', 'y', 'z'], list(c.keys()))
        self.assertEqual([81, 59, 32], list(c.values()))
        self.assertEqual([13], disposed_values)

        disposed_values = []
        c.clear()
        self.assertEqual([], list(c.keys()))
        self.assertEqual([], list(c.values()))
        self.assertEqual([81, 59, 32], disposed_values)

    def test_default_dispose(self):
        c = LruCache[str, str]()
        self.assertIsNone(c.dispose_value(13))

        class MyCache(LruCache[str, str]):
            disposed_values = []

            def dispose_value(self, value: str):
                self.disposed_values.append(value)

        my_c = MyCache()
        my_c.put('x', 'A')
        my_c.put('y', 'B')
        my_c.put('z', 'C')
        my_c.clear()
        self.assertEqual(['C', 'B', 'A'], my_c.disposed_values)

    def test_mapping_interface(self):
        c = LruCache[str, int]()

        c.put('x', 13)
        c.put('y', 58)

        # __getitem__
        self.assertEqual(13, c['x'])
        self.assertEqual(58, c['y'])
        with pytest.raises(KeyError, match="'z'"):
            v = c['z']

        # __iter__
        self.assertEqual(['y', 'x'], list(iter(c)))

        # __len__
        self.assertEqual(2, len(c))

        # __contains__
        self.assertTrue('x' in c)
        self.assertTrue('y' in c)
        self.assertFalse('z' in c)

    # noinspection PyMethodMayBeStatic
    def test_not_serializable(self):
        c = LruCache()
        with pytest.raises(RuntimeError,
                           match='Something went wrong:'
                                 ' objects of type LruCache are'
                                 ' not serializable'):
            pickle.dumps(c)
