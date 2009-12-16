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
