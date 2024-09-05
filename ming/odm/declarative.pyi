from typing import Any

from ming.odm.mapper import Query
from ming.encryption import EncryptedMixin

class MappedClass(EncryptedMixin):
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

    # from ming.odm.mapper.Mapper._instrumentation
    def __getitem__(self, item) -> Any: ...
    def __setitem__(self, key, value): ...
    def __delitem__(self, key): ...
    def __repr__(self) -> str: ...
    def delete(self) -> None: ...


def __getattr__(name) -> Any: ...  # marks file as incomplete
