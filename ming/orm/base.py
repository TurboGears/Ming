from .icollection import instrument

def state(obj):
    '''The state of a mapped object'''
    return obj.__ming__.state

def session(v):
    '''The ORMSession object managing either a class or an instance'''
    if isinstance(v, type):
        return v.query.mapper.session
    else:
        return session(type(v))

def lookup_class(name):
    from .mapped_class import MappedClass
    try:
        return MappedClass._registry[name]
    except KeyError:
        for n, cls in MappedClass._registry.iteritems():
            if n.endswith('.' + name): return cls
        raise

class ObjectState(object):
    new, clean, dirty, deleted = 'new clean dirty deleted'.split()

    def __init__(self):
        self._status = self.new
        self.original_document = None # unvalidated, as loaded from mongodb
        self.document = None
        self.extra_state = {}
        self.tracker = None

    def soil(self):
        if self.status == self.clean:
            self.status = self.dirty

    def validate(self, schema):
        status = self._status
        self.document = instrument(
            schema.validate(self.document),
            self.tracker)
        self._status = status

    def _get_status(self):
        return self._status
    def _set_status(self, value):
        self._status = value
    status = property(_get_status, _set_status)

    def __repr__(self):
        return '<ObjectState status=%s>' % self.status

class DocumentTracker(object):
    __slots__ = ('state',)

    def __init__(self, state):
        self.state = state

    def soil(self, value):
        self.state.soil()
    added_item = soil
    removed_item = soil
    cleared = soil

