from copy import deepcopy

def state(obj):
    '''The state of a mapped object'''
    return obj.__ming__.state

def session(v):
    '''The ORMSession object managing either a class or an instance'''
    if isinstance(v, type):
        return v.query.mapper.session
    else:
        return session(type(v))

class with_hooks(object):
    'Decorator to use for Session/Mapper extensions'

    def __init__(self, hook_name):
        self.hook_name = hook_name

    def __call__(self, func):
        before_meth = 'before_' + self.hook_name
        after_meth = 'after_' + self.hook_name
        def before(obj, *args, **kwargs):
            for e in obj.extensions:
                getattr(e, before_meth)(*args, **kwargs)
        def after(obj, *args, **kwargs):
            for e in obj.extensions:
                getattr(e, after_meth)(*args, **kwargs)
        def inner(obj, *args, **kwargs):
            before(obj, *args, **kwargs)
            result = func(obj, *args, **kwargs)
            after(obj, *args, **kwargs)
            return result
        inner.__name__ = func.__name__
        inner.__doc__ = 'Hook wraper around\n' + repr(func.__doc__)
        return inner

class ObjectState(object):
    new, clean, dirty, deleted = 'new clean dirty deleted'.split()

    def __init__(self):
        self._status = self.new
        self.original_document = None # unvalidated, as loaded from mongodb
        self.document = None
        self.extra_state = {}
        self.tracker = _DocumentTracker(self)

    def soil(self):
        if self.status == self.clean:
            self.status = self.dirty

    def validate(self, schema):
        status = self._status
        self.document = schema.validate(self.document)
        self._status = status

    def clone(self):
        return deepcopy(self.document)

    def _get_status(self):
        return self._status
    def _set_status(self, value):
        self._status = value
    status = property(_get_status, _set_status)

    def __repr__(self):
        return '<ObjectState status=%s>' % self.status

class _DocumentTracker(object):
    __slots__ = ('state',)

    def __init__(self, state):
        self.state = state

    def soil(self, value):
        self.state.soil()
    added_items = soil
    added_item = soil
    removed_item = soil
    cleared = soil

