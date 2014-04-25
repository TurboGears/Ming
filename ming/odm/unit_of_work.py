from ming.utils import indent
from .base import state, ObjectState
import six

class UnitOfWork(object):

    def __init__(self, session):
        self.session = session
        self._objects = {}

    def __iter__(self):
        return six.itervalues(self._objects)

    def save(self, obj):
        self._objects[id(obj)] = obj

    @property
    def new(self):
        return (obj for obj in six.itervalues(self._objects)
                if state(obj).status == ObjectState.new)

    @property
    def clean(self):
        return (obj for obj in six.itervalues(self._objects)
                if state(obj).status == ObjectState.clean)

    @property
    def dirty(self):
        return (obj for obj in six.itervalues(self._objects)
                if state(obj).status == ObjectState.dirty)

    @property
    def deleted(self):
        return (obj for obj in six.itervalues(self._objects)
                if state(obj).status == ObjectState.deleted)

    def flush(self):
        new_objs = {}
        inow = self.session.insert_now
        unow = self.session.update_now
        dnow = self.session.delete_now
        for i, obj in self._objects.items():
            st = state(obj)
            if st.status == ObjectState.new:
                inow(obj, st)
                st.status = ObjectState.clean
                new_objs[i] = obj
            elif st.status == ObjectState.dirty:
                unow(obj, st)
                st.status = ObjectState.clean
                new_objs[i] = obj
            elif st.status == ObjectState.deleted:
                dnow(obj, st)
            elif st.status == ObjectState.clean:
                new_objs[i] = obj
            else:
                assert False, 'Unknown obj state: %s' % st.status
        self._objects = new_objs
        self.session.imap.clear()
        for obj in six.itervalues(new_objs):
            self.session.imap.save(obj)

    def __repr__(self):
        l = ['<UnitOfWork>']
        l.append('  <new>')
        l += [ '    %s' % indent(repr(x), 6) for x in self.new ]
        l.append('  <clean>')
        l += [ '    %s' % indent(repr(x), 6) for x in self.clean ]
        l.append('  <dirty>')
        l += [ '    %s' % indent(repr(x), 6) for x in self.dirty ]
        l.append('  <deleted>')
        l += [ '    %s' % indent(repr(x), 6) for x in self.deleted ]
        return '\n'.join(l)

    def clear(self):
        self._objects = {} # dict[id(obj)] = obj

    def expunge(self, obj):
        try:
            del self._objects[id(obj)]
        except KeyError:
            pass

