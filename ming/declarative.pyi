from typing import TypeVar, Mapping, Any

from ming.base import Object
from ming.metadata import Manager

M = TypeVar('M')


# should actually be the following, but pycharm doesn't like to recognize underscore-prefixed names in .pyi files?
#   from ming.metadata import _Document
#   class _Document(Object): ...methods...
#   class Document(_Document):
class Document(Object):
    def __init__(self, data:Mapping=None, skip_from_bson=False) -> None: ...

    @classmethod
    def make(cls, data, allow_extra=False, strip_extra=True) -> Document: ...

    # ...
    # class __mongometa__:
    #     name: Any = ...
    #     session: Any = ...
    #     indexes: Any = ...

    m: Manager[Document]

    """
    The 'm' type above is not specific enough.  How can an owning class know what type its .m. deals with?  Other
    ORMs use mypy plugins:

    django-stubs:
        has BaseManager[Any] not parametrized to the owning model type
            https://github.com/typeddjango/django-stubs/blob/master/django-stubs/db/models/base.pyi#L25
        has a mypy plugin to add a lot of contextual information

    sqlalchemy-stubs:
        doesn't use quite so magical of a descriptor property, instead has:
            session.query(MyClass).filter(...)
        typed with:
            def query(self, entity: Union[Type[_T], Column[_T]], **kwargs) -> Query[_T]: ...
        has a mypy plugin too

    https://mypy.readthedocs.io/en/stable/more_types.html#precise-typing-of-alternative-constructors
        implies we could do this, but doesn't work for pycharm:
        BT = TypeVar('BT', bound='Document')
        m: Manager[BT]

    Our approach: in each model class, specify what its manager is, like this.  Its in quotes since Manager isn't a real class and has to be conditionally imported
        if typing.TYPE_CHECKING:
            from ming.metadata import Manager

        class WikiPage(...):
            ...
            m: 'Manager[WikiPage]'
    """

def __getattr__(name) -> Any: ...  # marks file as incomplete