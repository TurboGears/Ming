from ming.base import Document, DocumentMeta

from .base import mapper
from .property import ORMProperty

class Mapper(object):

    def __init__(self, mapped_class, dct):
        self._mapped_class = mapped_class
        self._dct = dct
        self._compiled = False
        # Setup properties
        self.property_index = dict(
            (k,v) for k,v in dct.iteritems()
            if isinstance(v, ORMProperty))
        for k,v in self.property_index.iteritems():
            v.name = k
            v.cls = mapped_class

    @property
    def properties(self):
        return self.property_index.itervalues()

    def compile(self):
        if self._compiled: return
        for p in self.properties:
            p.compile()
        self.doc_cls = make_document_class(self._mapped_class, self._dct)
        self._compiled = True

    def insert(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.insert(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        result = session.impl.insert(doc)
        if '_id' in doc:
            state.document['_id'] = doc._id

    def update(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.update(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        result = session.impl.save(doc)
        if '_id' in doc:
            state.document['_id'] = doc._id

    def delete(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.delete(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        result = session.impl.delete(doc)

def make_document_class(mapped_class, dct):
    name = '_ming_document_' + mapped_class.__name__
    bases = mapped_class.__bases__
    doc_bases = tuple( mapper(base).doc_cls
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

