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

    def __init__(self, cls, *args, **kwargs):
        self._cls = cls
        self._args = args
        self._kwargs = kwargs
        self._registry = local()

    def _get(self):
        if hasattr(self._registry, 'value'):
            result = self._registry.value
        else:
            result = self._cls(*self._args, **self._kwargs)
            self._registry.value = result
        return result

    def __getattr__(self, name):
        return getattr(self._get(), name)

    def __repr__(self):
        return 'TLProxy of %r' % self._get()

    def close(self):
        try:
            del self._registry.value
        except AttributeError:
            pass

def encode_keys(d):
    '''Encodes the unicode keys of d, making the result
    a valid kwargs argument'''
    return dict(
        (k.encode('utf-8'), v)
        for k,v in d.iteritems())

def all_class_properties(cls):
    'Find all properties of the class, including those inherited'
    found_names = set()
    for base in cls.__mro__:
        for k,v in base.__dict__.iteritems():
            if k in found_names: continue
            yield k,v
            found_names.add(k)

def wordwrap(s, width=80, indent_first=0, indent_subsequent=0):
    lines = []
    curline = [ ]
    indent = indent_first
    nchar = indent
    for word in s.split(' '):
        chars_to_add = len(word) if not curline else len(word) + 1
        if nchar + chars_to_add > width and curline:
            # wrap!
            lines.append(indent * ' ' + ' '.join(curline))
            indent = indent_subsequent
            curline = [ word ]
            nchar = indent + len(word)
        else:
            curline.append(word)
            nchar += chars_to_add
    lines.append(indent * ' ' + ' '.join(curline))
    return '\n'.join(lines)

def indent(s, level=2):
    prefix = ' ' * level
    return s.replace('\n', '\n' + prefix)
