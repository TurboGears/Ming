from .icollection import instrument

def state(obj):
    return obj.__ming__.state

def mapper(v):
    if isinstance(v, type):
        return v.__ming__.mapper
    else:
        return mapper(type(v))

def session(v):
    if isinstance(v, type):
        return v.__mongometa__.session
    else:
        return session(type(v))

class Decoration(object):

    def __init__(self, obj, bson):
        self.obj = obj
        doc_cls = mapper(obj).doc_cls
        self.state = ObjectState()
        doc = instrument(doc_cls.make(bson),
                         DocumentTracker(self.state))
        self.state.document = doc

class ObjectState(object):
    new, clean, dirty, deleted = 'new clean dirty deleted'.split()

    def __init__(self):
        self.status = self.new
        self.document = None
        self.extra_state = {}

    def soil(self):
        if self.status == self.clean:
            self.status = self.dirty

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

