from unittest import TestCase

from webtest import TestApp
from webob import exc

from ming import schema as S
from ming import datastore as DS
from ming import Session
from ming.orm import ThreadLocalORMSession
from ming.orm import FieldProperty
from ming.orm import MappedClass
from ming.orm.middleware import MingMiddleware

class TestRelation(TestCase):

    def setUp(self):
        self.datastore = DS.DataStore(
            master='mongo://localhost:27017/test_db')
        self.session = ThreadLocalORMSession(Session(bind=self.datastore))
        class Parent(MappedClass):
            class __mongometa__:
                name='parent'
                session = self.session
            _id = FieldProperty(S.ObjectId)
        MappedClass.compile_all()
        self.Parent = Parent
        self.session.impl.remove(self.Parent, {})
        self.create_app =  TestApp(MingMiddleware(self._wsgi_create_object))
        self.remove_app =  TestApp(MingMiddleware(self._wsgi_remove_object))
        self.remove_exc =  TestApp(MingMiddleware(self._wsgi_remove_object_exc))

    def test_create_flush(self):
        self.create_app.get('/')
        assert self.Parent.query.find().count() == 1

    def test_create_remove(self):
        self.create_app.get('/')
        self.remove_app.get('/')
        assert self.Parent.query.find().count() == 0

    def test_rollback(self):
        self.create_app.get('/')
        try:
            self.remove_exc.get('/')
        except AssertionError:
            pass
        assert self.Parent.query.find().count() == 1

    def _wsgi_create_object(self, environ, start_response):
        self.Parent()
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return ['Test']

    def _wsgi_remove_object(self, environ, start_response):
        p = self.Parent.query.get()
        p.delete()
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return ['Test']
    
    def _wsgi_remove_object_exc(self, environ, start_response):
        p = self.Parent.query.get()
        p.delete()
        err = exc.HTTPServerError('Test Error')
        assert False
    
