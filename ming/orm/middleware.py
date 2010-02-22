from webob import exc

from ming.orm import ThreadLocalORMSession

class MingMiddleware(object):

    def __init__(self, app, flush_on_errors=(exc.HTTPRedirection,)):
        self.app = app
        self.flush_on_errors = flush_on_errors

    def __call__(self, environ, start_response):
        try:
            result = self.app(environ, start_response)
            if isinstance(result, list):
                self._cleanup_request()
                return result
            else:
                return self._cleanup_iterator(result)
        except self.flush_on_errors, exc:
            self._cleanup_request()
            raise
        except:
            ThreadLocalORMSession.close_all()
            raise

    def _cleanup_request(self):
        ThreadLocalORMSession.flush_all()
        ThreadLocalORMSession.close_all()

    def _cleanup_iterator(self, result):
        for x in result:
            yield x
        self._cleanup_request()
    

def make_ming_autoflush_middleware(global_conf, **app_conf):
    def _filter(app):
        return MingMiddleware(app, **app_conf)
    return _filter
