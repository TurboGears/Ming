from ming.utils import indent
import six

class IdentityMap(object):

    def __init__(self):
        self._objects = {}

    def get(self, cls, id):
        return self._objects.get((cls, id), None)

    def save(self, value):
        vid = getattr(value, '_id', ())
        if vid is not ():
            self._objects[value.__class__, vid] = value

    def clear(self):
        self._objects = {}

    def expunge(self, obj):
        vid = getattr(obj, '_id', ())
        if vid is (): return
        try:
            del self._objects[(obj.__class__, vid)]
        except KeyError:
            pass

    def __iter__(self):
        for (cls,vid), value in six.iteritems(self._objects):
            yield cls, vid, value

    def __repr__(self):
        l = [ '<imap (%d)>' % len(self._objects) ]
        for k,v in six.iteritems(self._objects):
            l.append(indent('  %s : %s => %r'
                            % (k[0].__name__, k[1], v),
                            4))
        return '\n'.join(l)
