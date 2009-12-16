class IdentityMap(object):

    def __init__(self):
        self._objects = {}

    def get(self, cls, id):
        return self._objects.get((cls, id), None)

    def save(self, value):
        if '_id' in value:
            self._objects[value.__class__, value._id] = value

    def clear(self):
        self._objects = {}

    def __repr__(self):
        l = [ '<imap>' ]
        for k,v in sorted(self._objects.iteritems()):
            l.append('%s : %s => %r' % (
                    k[0], k[1], v))
        return '\n'.join(l)
