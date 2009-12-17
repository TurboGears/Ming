import cgi
import urllib
from threading import local

def parse_uri(uri, **kwargs):
    scheme, rest = urllib.splittype(uri)
    host, rest = urllib.splithost(rest)
    user, host = urllib.splituser(host)
    if user:
        username, password = urllib.splitpasswd(user)
    else:
        username = password = None
    host, port = urllib.splitnport(host)
    path, query = urllib.splitquery(rest)
    if query:
        kwargs.update(dict(cgi.parse_qsl(query)))
    return dict(
        scheme=scheme,
        host=host,
        username=username,
        password=password,
        port=port,
        path=path,
        query=kwargs)

class EmptyClass(object): pass

class LazyProperty(object):

    def __init__(self, func):
        self._func = func
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__

    def __get__(self, obj, klass=None):
        if obj is None: return None
        result = obj.__dict__[self.__name__] = self._func(obj)
        return result

class ThreadLocalProxy(object):
    _registry = local()

    def __init__(self, cls, *args, **kwargs):
        self._cls = cls
        self._args = args
        self._kwargs = kwargs

    def _get(self):
        if hasattr(self._registry, 'value'):
            result = self._registry.value
        else:
            result = self._cls(*self._args, **self._kwargs)
            self._registry.value = result
        return result

    def __getattr__(self, name):
        return getattr(self._get(), name)

    def close(self):
        # actually delete the tl session
        del self._registry.value

def encode_keys(d):
    '''Encodes the unicode keys of d, making the result
    a valid kwargs argument'''
    return dict(
        (k.encode('utf-8'), v)
        for k,v in d.iteritems())
