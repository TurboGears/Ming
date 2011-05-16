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
    added_item = soil
    removed_item = soil
    cleared = soil

