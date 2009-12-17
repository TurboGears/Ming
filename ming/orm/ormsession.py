from ming.session import Session
from .unit_of_work import UnitOfWork
from .identity_map import IdentityMap

class InstrumentedDict(dict):
    _onchange=None

    def __init__(self, *l, **kw):
        if self._onchange: self._onchange(self)
        dict.__init__(self, *l, **kw)

    def __setitem__(self, name, value):
        if self._onchange: self._onchange(self)
        return dict.__setitem__(self, name, value)

    def __delitem__(self, name):
        if self._onchange: self._onchange(self)
        return dict.__delitem__(self, name)

    def clear(self):
        if self._onchange: self._onchange(self)
        return dict.clear(self)

    def pop(self, k, *args):
        if self._onchange: self._onchange(self)
        return dict.pop(self, k, *args)

    def popitem(self):
        if self._onchange: self._onchange(self)
        return dict.popitem(self)

    def update(self, *args, **kwargs):
        if self._onchange: self._onchange(self)
        return dict.update(self, *args, **kwargs)

class InstrumentedList(list):
    _onchange=None

    def __init__(self, *l, **kw):
        if '_onchange' in kw:
            self._onchange = kw.pop('_onchange')
        if self._onchange: self._onchange(self)
        list.__init__(self, *l, **kw)

    def __setitem__(self, name, value):
        if self._onchange: self._onchange(self)
        return list.__setitem__(self, name, value)

    def __setslice__(self, name, value):
        if self._onchange: self._onchange(self)
        return list.__setslice__(self, name, value)

    def __delitem__(self, name):
        if self._onchange: self._onchange(self)
        return list.__delitem__(self, name)

    def __delslice__(self, name):
        if self._onchange: self._onchange(self)
        return list.__delslice__(self, name)

    def append(self, value):
        if self._onchange: self._onchange(self)
        return list.append(self, value)

    def extend(self, k, *args):
        if self._onchange: self._onchange(self)
        return list.extend(self, k, *args)

