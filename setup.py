from setuptools import setup, find_packages

__version__ = 'undefined'

exec(open('ming/version.py').read())

setup(name='Ming',
      version=__version__,
      description="Bringing order to Mongo since 2009",
      long_description="""Database mapping layer for MongoDB on Python. Includes schema enforcement and some facilities for schema migration.
""",
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
      ], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      python_requires='>=3.5',
      keywords='mongo, pymongo',
      author='Rick Copeland',
      author_email='rick@geek.net',
      url='https://github.com/TurboGears/Ming',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
        "pymongo>=3.0,<3.12",
        "pytz",
        "six>=1.6.1"
      ],
      tests_require=[
        "mock >=0.8.0",
        "pytz",
        "WebOb",
        "webtest",
        "FormEncode >= 1.2.1",
        # "python-spidermonkey >= 0.0.10", # required for full MIM functionality
      ],
      test_suite="ming.tests",
      extras_require={
        "configure": [
            "FormEncode >= 1.2.1",  # required to use ``ming.configure``
        ]
      },
      entry_points="""
      # -*- Entry points: -*-
      [paste.filter_factory]
      ming_autoflush=ming.odm.middleware:make_ming_autoflush_middleware
      """
)
