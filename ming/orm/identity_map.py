from ming.utils import indent

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

    def __repr__(self):
        l = [ '<imap>' ]
        for k,v in sorted(self._objects.iteritems()):
            l.append(indent('  %s : %s => %r'
                            % (k[0].__name__, k[1], v),
                            4))
        return '\n'.join(l)
