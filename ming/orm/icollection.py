from mock import Mock

def instrument(obj, tracker):
    if isinstance(obj, dict):
        return InstrumentedObj(obj, tracker)
    elif isinstance(obj, list):
        return InstrumentedList(obj, tracker)
    else:
        return obj

def deinstrument(obj):
    if hasattr(obj, '_deinstrument'):
        return obj._deinstrument()
    else:
        return obj

def full_deinstrument(obj):
    obj = deinstrument(obj)
    if isinstance(obj, Mock):
        return obj
    elif hasattr(obj, 'iteritems'):
        return dict(
            (k, full_deinstrument(v))
            for k,v in obj.iteritems())
        return full_deinstrument(obj)
    elif hasattr(obj, 'extend'):
        return list(full_deinstrument(o) for o in obj)
    else:
        return obj

class InstrumentedProxy(object):
    _impl = None
    _tracker = None

    def __init__(self, impl, tracker):
        self._impl = impl
        self._tracker = tracker

    def __repr__(self):
        return 'I' + repr(self._impl)

    def _instrument(self, v):
        return instrument(v, self._tracker)

    def _deinstrument(self):
        return self._impl

    def __eq__(self, o):
        return self._impl == o

    def __len__(self):
        return len(self._impl)

    def __json__(self):
        return self._impl

class InstrumentedObj(InstrumentedProxy):

    def __getitem__(self, name):
        return self._instrument(self._impl[name])

    def __setitem__(self, name, value):
        value = deinstrument(value)
        old_value = self._impl.get(name, ())
        self._impl[name] = value
        if old_value is not ():
            self._tracker.removed_item(old_value)
        self._tracker.added_item(value)

    def __delitem__(self, name):
        value = self._impl[name]
        del self._impl[name]
        self._tracker.removed_item(value)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError, name

    def __setattr__(self, name, value):
        if hasattr(self.__class__, name):
            super(InstrumentedObj, self).__setattr__(name, value)
        else:
            self.__setitem__(name, value)

    def __contains__(self, k):
        return k in self._impl

    def __iter__(self):
        return iter(self._impl)

    def keys(self):
        return self._impl.keys()

    def values(self):
        return map(self._instrument, self._impl.values())

    def items(self):
        return [ (k, self._instrument(v)) for k,v in self._impl.iteritems() ]

    def iterkeys(self):
        return self._impl.iterkeys()

    def itervalues(self):
        return ( self._instrument(v) for v in self._impl.itervalues() )

    def iteritems(self):
        return ( (k, self._instrument(v)) for k,v in self._impl.iteritems() )

    def get(self, k, default=None):
        return self._instrument(self._impl.get(k, default))

    def setdefault(self, k, default=None):
        return self._instrument(self._impl.setdefault(k, deinstrument(default)))

    def clear(self):
        self._impl.clear()
        self._tracker.cleared()

    def pop(self, k, *args):
        result = self._impl.pop(k, *args)
        self._tracker.removed_item(result)
        return result

    def popitem(self):
        k,v = self._impl.popitem()
        self._tracker.removed_item(v)
        return k,v

    def update(self, *args, **kwargs):
        'Must do all the work ourselves so we track the related objects'
        if len(args) == 1:
            arg = args[0]
            if isinstance(arg, dict): arg = arg.iteritems()
            for k,v in arg:
                self[k] = v
        for k,v in kwargs.iteritems():
            self[k] = v

    def replace(self, other):
        self.clear()
        self.update(other)

class InstrumentedList(InstrumentedProxy):

    def __getitem__(self, i):
        return self._instrument(self._impl[i])

    def __setitem__(self, i, value):
        value = deinstrument(value)
        oldvalue = self[i]
        self._impl[i] = value
        self._tracker.removed_item(oldvalue)
        self._tracker.added_item(value)

    def __delitem__(self, i):
        item = self[i]
        del self._impl[i]
        self._tracker.removed_item(item)

    def __getslice__(self, i, j):
        return self._instrument(self._impl[i:j])
    
    def __setslice__(self, i, j, value):
        value = deinstrument(value)
        old_items = self._impl[i:j]
        self._impl[i:j] = value
        for item in old_items:
            self._tracker.removed_item(item)
        for item in self._impl[i:j]:
            self._tracker.added_item(item)

    def __delslice__(self, i, j):
        for item in self._impl[i:j]:
            self._tracker.removed_item(item)
        del self._impl[i:j]

    def __iadd__(self, y):
        self.extend(y)
        return self

    def __add__(self, other):
        new_impl = list(self)
        new_impl.extend(other)
        return self._instrument(new_impl)

    def __radd__(self, other):
        new_impl = list(other)
        new_impl.extend(self)
        return self._instrument(new_impl)

    def __imul__(self, y):
        if y <= 0:
            self[:] = []
        else:
            orig = self._impl
            for i in range(1,y):
                self.extend(list(orig))
        return self

    def __iter__(self):
        return (self._instrument(v) for v in self._impl)

    def append(self, value):
        value = deinstrument(value)
        self._impl.append(value)
        self._tracker.added_item(value)

    def extend(self, iterable):
        for item in iterable:
            self.append(item)

    def insert(self, index, value):
        value = deinstrument(value)
        self._impl.insert(index, value)
        self._tracker.added_item(value)

    def pop(self, index=()):
        if index is ():
            result = self._impl.pop()
        else:
            result = self._impl.pop(index)
        self._tracker.removed_item(result)
        return self._instrument(result)
           
    def remove(self, value):
        try:
            index = self.index(value)
        except ValueError:
            raise ValueError, 'InstrumentedList.remove(x): x not in list'
        del self[index]
        
    def replace(self, other):
        while self:
            self.pop()
        self.extend(other)

    def index(self, value):
        value = deinstrument(value)
        return self._impl.index(value)
