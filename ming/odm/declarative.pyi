from typing import Any

from ming.odm.mapper import Query

class MappedClass:
    def __init__(self, **kwargs) -> None: ...

    query: Query[MappedClass]

    """
        in each model class, specify what its manager is, like this.
        Its in quotes since Manager isn't a real class and has to be conditionally imported
        if typing.TYPE_CHECKING:
            from ming.metadata import Manager

        class WikiPage(...):
            ...
            m: 'Manager[WikiPage]'
    """

    def __getitem__(self, item) -> Any: ...
    def __setitem__(self, key, value): ...
    def __delitem__(self, key): ...


def __getattr__(name) -> Any: ...  # marks file as incomplete