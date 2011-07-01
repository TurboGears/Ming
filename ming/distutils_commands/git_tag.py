import os

from setuptools import Command

class git_tag(Command):
    description = "tag a release"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        tagname = self.distribution.get_version()
        cmd = 'git tag %s' % tagname
        os.system(cmd)

