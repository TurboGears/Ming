
# re-use all the Field type handling
from collections.abc import Iterable
from typing import overload, Union, Type, Any, TypeVar, TypeAlias

from bson import ObjectId

from ming.metadata import Field as FieldProperty
from ming.metadata import Field as FieldPropertyWithMissingNone
from ming.odm.declarative import MappedClass

MC = TypeVar('MC', bound=MappedClass)

class ForeignIdProperty:
    @overload
    def __new__(self, related: Type[MC], uselist=False, allow_none=False, *args, **kwargs) -> ObjectId: ...
    @overload
    def __new__(self, related: str, uselist=False, allow_none=False, *args, **kwargs) -> ObjectId: ...

class RelationProperty:
    @overload
    def __new__(self, related: str, via: str=None, fetch=True) -> Any:...
    @overload
    def __new__(self, related: Type[MC], via: str=None, fetch=True) -> MC:...
    @overload
    def __new__(self, related: Type[MC], via: str=None, fetch=True) -> Iterable[MC]:...


def __getattr__(name) -> Any: ...  # marks file as incomplete