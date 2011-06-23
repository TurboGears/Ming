import os

from setuptools import setup, find_packages, Command

__version__ = 'undefined'

exec open('ming/version.py')

class tag(Command):
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

class sf_upload(Command):
    description = "upload a release to SourceForge"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import paramiko
        ssh = paramiko.SSHClient()
        host = 'frs.sourceforge.net'
        username='rick446,merciless'
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, username=username)
        sftp = ssh.open_sftp()
        sftp.chdir('/home/frs/project/m/me/merciless')
        for cmd, _, filename in self.distribution.dist_files:
            basename = os.path.basename(filename)
            dirname = self.distribution.get_version()
            if dirname not in sftp.listdir():
                sftp.mkdir(dirname)
            sftp.put(filename, '%s/%s' % (dirname, basename))

setup(name='Ming',
      version=__version__,
      description="Bringing order to Mongo since 2009",
      long_description="""Database mapping layer for MongoDB on Python. Includes schema enforcement and some facilities for schema migration. 
""",
      cmdclass = dict(
        tag=tag, sf_upload=sf_upload),
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='mongo, pymongo',
      author='Rick Copeland',
      author_email='rick@geek.net',
      url='http://merciless.sourceforge.net',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
          # -*- Extra requirements: -*-
        "mock >= 0.6.0",
        "FormEncode >= 1.2.1",
        "pymongo >= 1.9",
        # "python-spidermonkey >= 0.0.10", # required for full MIM functionality
      ],
      entry_points="""
      # -*- Entry points: -*-
      [paste.filter_factory]
      ming_autoflush=ming.orm.middleware:make_ming_autoflush_middleware

      [flyway.test_migrations]
      a = flyway.tests.migrations_a
      b = flyway.tests.migrations_b

      [paste.paster_command]
      flyway = flyway.command:MigrateCommand
      """,
      test_suite='nose.collector'
      )
