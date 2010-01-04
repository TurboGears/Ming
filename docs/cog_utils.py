import cog
import sys
import code
import inspect
import textwrap
from StringIO import StringIO

snippet_namespace = {}

def interact(mod, snippet):
    exec ('from %s import *' % mod) in snippet_namespace, snippet_namespace
    skipping = True
    script = extract_session(snippet_namespace, 'snippet%d' % snippet)
    # Capture stdout, stderr
    old_stdin, old_stdout, old_stderr = sys.stdin, sys.stdout, sys.stderr
    sys.stdout = sys.stderr = stdout = StringIO()
    sys.stdin = EchoingStringIO(stdout, script)
    console = code.InteractiveConsole(snippet_namespace)
    console.interact('')
    sys.stdin, sys.stdout, sys.stderr = old_stdin, old_stdout, old_stderr
    # cog.out('.. ' + script.replace('\n', '\n.. '))
    output = stdout.getvalue()[:-5]
    cog.out(output)
    cog.outl()

def extract_session(namespace, func):
    func = namespace[func]
    func_src = inspect.getsource(func)
    func_src_lines = func_src.split('\n')[1:]
    session = textwrap.dedent('\n'.join(func_src_lines))
    return session

class EchoingStringIO(StringIO):

    def __init__(self, stdout, *args, **kwargs):
        self._stdout = stdout
        StringIO.__init__(self, *args, **kwargs)

    def read(self, *args, **kwargs):
        assert False
        result = StringIO.read(self, *args, **kwargs)
        self._stdout.write(result)
        return result

    def readline(self, *args, **kwargs):
        result = StringIO.readline(self, *args, **kwargs)
        self._stdout.write(result)
        return result
    
