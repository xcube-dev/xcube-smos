# The MIT License (MIT)
# Copyright (c) 2023 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import collections
import collections.abc
import threading
from typing import TypeVar, Generic, Dict, Any, Callable, Optional, \
    Deque, Iterator

from xcube.util.assertions import assert_instance, assert_true


class NotSerializable:
    """A mixin that avoids serialization."""

    def __getstate__(self):
        raise RuntimeError(f"Something went wrong:"
                           f" objects of type {self.__class__.__name__}"
                           f" are not serializable")


KT = TypeVar('KT')
VT = TypeVar('VT')


class LruCache(Generic[KT, VT],
               NotSerializable,
               collections.abc.Mapping[KT, VT]):
    def __init__(self,
                 max_size: int = 128,
                 dispose_value: Optional[Callable[[VT], Any]] = None):
        assert_instance(max_size, int, name="max_size")
        assert_true(max_size >= 0,
                    message="max_size must be greater or equal zero")
        if dispose_value is None:
            dispose_value = self.dispose_value
        else:
            assert_true(callable(dispose_value),
                        message="dispose_value must be callable")
        self._max_size = max_size
        self._dispose_value = dispose_value
        self._keys: Deque[KT] = collections.deque([], max_size)
        self._values: Dict[KT, VT] = {}
        self._lock = threading.RLock()
        self._undefined = object()

    ##########################################
    # Mapping interface

    def __len__(self) -> int:
        return self.size

    def __iter__(self) -> Iterator[KT]:
        yield from self.keys()

    def __contains__(self, key: KT) -> bool:
        return key in self._values

    def __getitem__(self, key: KT) -> VT:
        value = self.get(key, self._undefined)
        if value is self._undefined:
            raise KeyError(key)
        return value

    ##########################################
    # LruCache interface

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def size(self) -> int:
        return len(self._keys)

    def keys(self) -> Iterator[KT]:
        yield from self._keys

    def values(self) -> Iterator[VT]:
        for k in self._keys:
            yield self._values[k]

    def get(self, key: KT, default: Optional[VT] = None) -> VT:
        if not self._max_size:
            return default
        value = self._values.get(key, self._undefined)
        if value is self._undefined:
            return default
        if self._keys[0] != key:
            # if not LRU yet, make it LRU
            self.put(key, value)
        return value

    def put(self, key: KT, value: VT):
        if not self._max_size:
            return
        with self._lock:
            if key in self._values:
                prev_value = self._values[key]
                if prev_value is not value:
                    self._dispose_value(prev_value)
                self._keys.remove(key)
            elif self.size == self.max_size:
                oldest_key = self._keys.pop()
                oldest_value = self._values.pop(oldest_key)
                self._dispose_value(oldest_value)
            self._keys.appendleft(key)
            self._values[key] = value

    def clear(self):
        with self._lock:
            if self._dispose_value is not self.dispose_value:
                values = list(self.values())
            else:
                values = []
            self._keys.clear()
            self._values.clear()
            for value in values:
                self._dispose_value(value)

    def dispose_value(self, value: VT):
        """May be overridden by subclasses."""
        pass
