class UnitOfWork(object):

    def __init__(self, session, autoflush=False):
        self.session = session
        self._autoflush = autoflush
        self._objects = {} # dict[id(obj)] = obj
        self._new = set()
        self._clean = set()
        self._dirty = set()
        self._deleted = set()

    @property
    def new(self):
        for oid in self._new:
            yield self._objects[oid]

    @property
    def clean(self):
        for oid in self._clean:
            yield self._objects[oid]

    @property
    def dirty(self):
        for oid in self._dirty:
            yield self._objects[oid]

    @property
    def deleted(self):
        for oid in self._deleted:
            yield self._objects[oid]

    def save_new(self, obj):
        if self._autoflush:
            return
        oid = id(obj)
        self._objects[oid] = obj
        self._new.add(oid)
        self._clean.discard(oid)
        self._dirty.discard(oid)
        self._deleted.discard(oid)

    def save_clean(self, obj):
        if self._autoflush:
            return
        oid = id(obj)
        self._objects[oid] = obj
        self._clean.add(oid)
        self._new.discard(oid)
        self._dirty.discard(oid)
        self._deleted.discard(oid)

    def save_dirty(self, obj):
        if self._autoflush:
            self.session.save(obj)
            return
        oid = id(obj)
        self._objects[oid] = obj
        self._dirty.add(oid)
        self._new.discard(oid)
        self._clean.discard(oid)
        self._deleted.discard(oid)

    def save_deleted(self, obj):
        if self._autoflush:
            self.session.delete(obj)
            return
        oid = id(obj)
        self._objects[oid] = obj
        self._deleted.add(oid)
        self._new.discard(oid)
        self._clean.discard(oid)
        self._dirty.discard(oid)

    def flush(self):
        for obj in list(self.new):
            self.session.save(obj)
        for obj in list(self.dirty):
            self.session.save(obj)
        for obj in list(self.deleted):
            self.session.delete(obj)
        self._clean |= self._new
        self._clean |= self._dirty
        self._new = set()
        self._dirty = set()
        self._deleted = set()
        for obj in self.clean:
            self.session.imap.save(obj)

    def __repr__(self):
        l = ['<UnitOfWork>']
        l.append('  <new>')
        l += [ '    %r' % x for x in self.new ]
        l.append('  <clean>')
        l += [ '    %r' % x for x in self.clean ]
        l.append('  <dirty>')
        l += [ '    %r' % x for x in self.dirty ]
        l.append('  <deleted>')
        l += [ '    %r' % x for x in self.deleted ]
        return '\n'.join(l)

    def clear(self):
        self._objects = {} # dict[id(obj)] = obj
        self._new = set()
        self._clean = set()
        self._dirty = set()
        self._deleted = set()

