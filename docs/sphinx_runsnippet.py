"""
    Shpinx run-pysnippet Directive
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    runs a function from a module inside an interactive python console
    and includes the console output inside documentation.
"""

import sys
import code
import inspect
import textwrap
from StringIO import StringIO
from docutils import nodes
from docutils.parsers.rst import Directive


def interact(mod, snippet_function):
    snippet_namespace = {}
    exec ('from %s import *' % mod) in snippet_namespace, snippet_namespace
    script = extract_session(snippet_namespace, snippet_function)
    # Capture stdout, stderr
    old_stdin, old_stdout, old_stderr = sys.stdin, sys.stdout, sys.stderr
    sys.stdout = sys.stderr = stdout = StringIO()
    sys.stdin = EchoingStringIO(stdout, script)
    console = code.InteractiveConsole(snippet_namespace)
    console.interact('')
    sys.stdin, sys.stdout, sys.stderr = old_stdin, old_stdout, old_stderr
    # cog.out('.. ' + script.replace('\n', '\n.. '))
    return stdout.getvalue()[:-5]


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

    def readline(self, *args, **kwargs):
        result = StringIO.readline(self, *args, **kwargs)
        self._stdout.write(result)
        return result


class IncludeInteractDirective(Directive):
    has_content = False
    required_arguments = 2

    def run(self):
        document = self.state.document
        env = document.settings.env

        pymodule = self.arguments[0]
        pyfuncion = self.arguments[1]

        text = interact(pymodule, pyfuncion).strip()
        retnode = nodes.literal_block(text, text, source=pymodule)
        # retnode['language'] = 'python-console'
        return [retnode]

def setup(app):
    app.add_directive('run-pysnippet', IncludeInteractDirective)

