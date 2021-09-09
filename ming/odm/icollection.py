import six

def instrument(obj, tracker):
    if isinstance(obj, (dict, list)):
        if hasattr(obj, '_ming_instrumentation'):
            return obj
        if isinstance(obj, dict):
            return InstrumentedObj(obj, tracker)
        else:
            return InstrumentedList(obj, tracker)
    else:
        return obj

def deinstrument(obj):
    if hasattr(obj, '_deinstrument'):
        return obj._deinstrument()
    else:
        return obj

class InstrumentedObj(dict):
    '''self is instrumented; _impl is not.'''
    __slots__ = ('_impl', '_tracker')
    _ming_instrumentation = True

    def __init__(self, impl, tracker):
        self._impl = impl
        self._tracker = tracker
        dict.update(
            self,
            ((k,instrument(v, self._tracker)) for k,v in six.iteritems(impl)))

    def _deinstrument(self):
        return self._impl

    def __repr__(self):
        return 'I' + repr(self._impl)

    def __delitem__(self, k):
        self.pop(k)

    def __setitem__(self, k, v):
        v = deinstrument(v)
        iv = instrument(v, self._tracker)
        self.pop(k, None)
        super(InstrumentedObj, self).__setitem__(k, iv)
        self._impl[k] = v
        self._tracker.added_item(v)

    def __setattr__(self, k, v):
        if hasattr(self.__class__, k):
            super(InstrumentedObj, self).__setattr__(k, v)
        else:
            self[k] = v

    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)

    def __eq__(self, y):
        return self._impl == deinstrument(y)

    def clear(self):
        super(InstrumentedObj, self).clear()
        self._impl.clear()
        self._tracker.cleared()

    def copy(self):
        return InstrumentedObj(self._impl.copy(), self._tracker)

    @classmethod
    def fromkeys(self, *args, **kwargs):
        return instrument(dict.fromkeys(*args, **kwargs))

    def pop(self, k, *args):
        value = self._impl.pop(k, *args)
        if k in self: self._tracker.removed_item(value)
        super(InstrumentedObj, self).pop(k, *args)
        return value

    def popitem(self):
        k,v = self._impl.popitem()
        super(InstrumentedObj, self).popitem()
        self._tracker.removed_item(v)
        return v

    def setdefault(self, k, *args):
        if k in self:
            return self.get(k)
        if len(args) == 1:
            self[k] = args[0]
            return self[k]
        raise (
            TypeError,
            'setdefault expected 1 or 2 arguments, got %d' % len(args)+1)

    def update(self, *args, **kwargs):
        if len(args) > 1:
            raise (
                TypeError,
                'update expected at most 1 arguments, got %d' % len(args)+1)
        elif args:
            E = args[0]
            if hasattr(E, 'keys'):
                for k in E:
                    self[k] = E[k]
            else:
                for k,v in E:
                    self[k] = v
        for k,v in six.iteritems(kwargs):
            self[k] = v

    def replace(self, v):
        self.clear()
        self.update(v)

class InstrumentedList(list):
    '''self is instrumented; _impl is not.'''
    __slots__ = ('_impl', '_tracker')
    _ming_instrumentation = True

    def __init__(self, impl, tracker):
        self._impl = impl
        self._tracker = tracker
        super(InstrumentedList, self).extend(
            instrument(item, self._tracker)
            for item in self._impl)

    def __repr__(self):
        return 'I' + repr(self._impl)

    def _deinstrument(self):
        return self._impl

    def __eq__(self, y):
        return self._impl == deinstrument(y)

    def __setitem__(self, key, v):
        v = deinstrument(v)
        iv = instrument(v, self._tracker)
        super(InstrumentedList, self).__setitem__(key, iv)

        if isinstance(key, slice):
            self._tracker.removed_items(self._impl[key.start:key.stop])
            self._impl[key.start:key.stop:key.step] = v
            self._tracker.added_items(v)
        else:
            i = key
            self._tracker.removed_item(self._impl[i])
            self._impl[i] = v
            self._tracker.added_item(self._impl[i])

    def __delitem__(self, key):
        super(InstrumentedList, self).__delitem__(key)

        if isinstance(key, slice):
            self._tracker.removed_items(self._impl[key.start:key.stop:key.step])
            del self._impl[key.start:key.stop:key.step]
        else:
            i = key
            self._tracker.removed_item(self._impl[i])
            del self._impl[i]

    def __add__(self, y):
        return instrument(self._impl + y, self._tracker)

    def __radd__(self, y):
        return instrument(y + self._impl, self._tracker)

    def __iadd__(self, y):
        self.extend(y)
        return self

    def __mul__(self, y):
        return instrument(self._impl * y, self._tracker)

    def __rmul__(self, y):
        return instrument(y * self._impl, self._tracker)

    def __imul__(self, y):
        if y <= 0:
            self[:] = []
        else:
            lst = list(self._impl)
            for x in range(y-1):
                self.extend(lst)
        return self

    def __contains__(self, v):
        v = deinstrument(v)
        return v in self._impl

    def append(self, v):
        v = deinstrument(v)
        iv =instrument(v, self._tracker)
        self._impl.append(v)
        super(InstrumentedList, self).append(iv)
        self._tracker.added_item(v)

    def extend(self, iterable):
        new_items = list(map(deinstrument, iterable))
        self._impl.extend(new_items)
        super(InstrumentedList, self).extend(
            instrument(item, self._tracker)
            for item in new_items)
        self._tracker.added_items(new_items)

    def insert(self, index, v):
        v = deinstrument(v)
        iv = instrument(v, self._tracker)
        super(InstrumentedList, self).insert(index, iv)
        self._impl.insert(index, v)
        self._tracker.added_item(v)

    def pop(self, pos=-1):
        v = self._impl.pop(pos)
        super(InstrumentedList, self).pop(pos)
        self._tracker.removed_item(v)
        return v

    def remove(self, v):
        v = deinstrument(v)
        i = self._impl.index(v)
        del self[i]

    def index(self, v, *args, **kwargs):
        v = deinstrument(v)
        return self._impl.index(v, *args, **kwargs)

    def replace(self, iterable):
        self[:] = iterable
