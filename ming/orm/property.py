from ming.base import Field
from .base import state

class ORMProperty(object):

    def __init__(self):
        self.name = None

    def __get__(self, instance, cls=None):
        raise NotImplementedError, '__get__'

    def __set__(self, instance, value):
        raise NotImplementedError, '__set__'

    def insert(self, mapper, session, instance, state):
        pass

    def update(self, mapper, session, instance, state):
        pass

    def delete(self, mapper, session, instance, state):
        pass

class FieldProperty(ORMProperty):

    def __init__(self, field_type, *args, **kwargs):
        ORMProperty.__init__(self)
        self.field_type = field_type
        self.args = args
        self.kwargs = kwargs
        self.field = Field(field_type, *args, **kwargs)
        self.include_in_repr = True

    def repr(self, doc):
        try:
            return repr(self.__get__(doc))
        except AttributeError:
            return '<Missing>'

    def __get__(self, instance, cls=None):
        st = state(instance)
        return getattr(st.document, self.name)

    def __set__(self, instance, value):
        st = state(instance)
        st.document[self.name] = value


