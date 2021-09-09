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
from io import StringIO
from docutils import nodes
from docutils.parsers.rst import Directive, directives
from sphinx.util import parselinenos


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
    option_spec = {
        'skip': int,
        'emphasize-lines': directives.unchanged_required,
    }

    def run(self):
        document = self.state.document
        env = document.settings.env

        pymodule = self.arguments[0]
        pyfuncion = self.arguments[1]

        text = interact(pymodule, pyfuncion).strip()

        skip = self.options.get('skip', 0)
        if skip:
            filtered_lines = []
            count = 0
            for line in text.split('\n'):
                if count >= skip:
                    filtered_lines.append(line)
                if line.startswith('>>>'):
                    count += 1
            text = '\n'.join(filtered_lines)

        hl_lines = None
        linespec = self.options.get('emphasize-lines')
        if linespec:
            lines = text.split('\n')
            try:
                hl_lines = [x+1 for x in parselinenos(linespec, len(lines))]
            except ValueError as err:
                return [document.reporter.warning(str(err), line=self.lineno)]

        retnode = nodes.literal_block(text, text, source=pymodule)

        if hl_lines is not None:
            retnode['highlight_args'] = {'hl_lines': hl_lines}

        return [retnode]

def setup(app):
    app.add_directive('run-pysnippet', IncludeInteractDirective)

