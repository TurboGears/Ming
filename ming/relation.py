from ming import Field, Document
from ming import schema
from ming.utils import LazyProperty

def relation(*args, **kwargs):
    return RelationDescriptor(*args, **kwargs)

class RelationDescriptor(Field):

    def __init__(self, related_classname, via, backref=None):
        self.related_classname = related_classname
        self.via = via
        self.backref = backref
        self.type = BackrefFixer(self)
        self.args, self.kwargs = (), {}

    @LazyProperty
    def related_class(self):
        return Document._registry[self.related_classname]
    
    def __get__(self, instance, cls):
        loader = get_loader(
            instance, id(self), OneToManyRelationLoader,
            self.related_class, self.via)
        return loader.get()

class BackrefFixer(schema.Deprecated):

    def __init__(self, relation):
        self.relation = relation

    def fixup_backref(self, cls):
        if self.relation.backref:
            setattr(self.relation.related_class,
                    self.relation.backref,
                    BackrefDescriptor(cls, self.relation.via))

class BackrefDescriptor(Field):

    def __init__(self, related_class, via):
        self.related_class = related_class
        self.via = via

    def __get__(self, instance, cls):
        loader = get_loader(
            instance, id(self), ManyToOneRelationLoader,
            self.related_class, self.via)
        return loader.get()

class ObjectState(object):

    def __init__(self):
        self.loaders = {}

    @classmethod
    def get(cls, instance):
        if '__mingstate__' in instance.__dict__:
            result = instance.__mingstate__
        else:
            result = ObjectState()
            result.__dict__['__mingstate__'] = result
        return result

class OneToManyRelationLoader(object):

    def __init__(self, instance, related_class, via):
        self._value = None
        self._loaded = False
        self.instance = instance
        self.related_class, self.via = \
            related_class, via

    def get(self):
        if self._loaded: return self._value
        self.load()
        return self._value

    def load(self):
        q = self.related_class.m.find({self.via:self.instance._id})
        self._value = q.all()

class ManyToOneRelationLoader(object):

    def __init__(self, instance, related_class, via):
        self._value = None
        self._loaded = False
        self.instance = instance
        self.related_class, self.via = \
            related_class, via

    def get(self):
        if self._loaded: return self._value
        self.load()
        return self._value

    def load(self):
        self._value = self.related_class.m.get(
            _id=getattr(self.instance, self.via))
        
def get_loader(instance, key, loader_cls, *args, **kwargs):
    state = ObjectState.get(instance)
    if key in state.loaders:
        result = state.loaders[key]
    else:
        result = state.loaders[key] = loader_cls(instance, *args, **kwargs)
    return result

