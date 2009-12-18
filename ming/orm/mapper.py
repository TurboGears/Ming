from ming.utils import all_class_properties
from ming.base import Document, DocumentMeta

from .base import mapper, state, session
from .property import ORMProperty

class Mapper(object):

    def __init__(self, mapped_class):
        self._mapped_class = mapped_class
        self._dct = dict(all_class_properties(mapped_class))
        self._compiled = False
        # Setup properties
        self.property_index = dict(
            (k,v) for k,v in self._dct.iteritems()
            if isinstance(v, ORMProperty))
        for k,v in self.property_index.iteritems():
            v.name = k
            v.cls = mapped_class

    def __repr__(self):
        return '<Mapper for %s>' % (
            self._mapped_class.__name__)

    @property
    def properties(self):
        return self.property_index.itervalues()

    def compile(self):
        if self._compiled: return self
        for p in self.properties:
            p.compile()
        self.doc_cls = make_document_class(self._mapped_class, self._dct)
        self._compiled = True
        self._mapped_class.__mongometa__ = self.doc_cls.__mongometa__
        return self

    def insert(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.insert(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        result = session.impl.insert(doc)
        if '_id' in doc:
            state.document['_id'] = doc._id
        session.save(obj)
        state.status = state.clean

    def update(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.update(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        result = session.impl.save(doc)
        if '_id' in doc:
            state.document['_id'] = doc._id
        state.status = state.clean

    def delete(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.delete(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        result = session.impl.delete(doc)
        session.expunge(obj)

    def remove(self, *args, **kwargs):
        session(self._mapped_class).remove(self._mapped_class, *args, **kwargs)

    def refresh(self, obj, states_to_refresh=None):
        st = state(obj)
        if states_to_refresh is None:
            states_to_refresh = (st.new, st.dirty, st.deleted)
        sess = obj.__mongometa__.session
        if st.status == st.new:
            self.insert(sess, obj, st)
        elif st.status == st.dirty:
            self.update(sess, obj, st)
        elif st.status == st.deleted:
            self.delete(sess, obj, st)

def make_document_class(mapped_class, dct):
    name = '_ming_document_' + mapped_class.__name__
    bases = mapped_class.__bases__
    doc_bases = tuple( mapper(base).compile().doc_cls
                       for base in bases
                       if hasattr(base, '__ming__') )
    if not doc_bases:
        doc_bases = (Document,)
    doc_dct = dict(
        (k, v.field)
        for k,v in dct.iteritems()
        if hasattr(v, 'field'))
    doc_dct['__mongometa__'] = dct['__mongometa__']
    return DocumentMeta(name, doc_bases, doc_dct)

