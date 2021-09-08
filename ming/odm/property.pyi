
# re-use all the Field type handling
from typing import overload, Union, Type, Any, TypeVar, TypeAlias

from bson import ObjectId

from ming.metadata import Field as FieldProperty
from ming.metadata import Field as FieldPropertyWithMissingNone
from ming.odm.declarative import MappedClass

MC = TypeVar('MC', bound=MappedClass)

class ForeignIdProperty:
    @overload
    def __new__(self, related: Type, uselist=False, allow_none=False, *args, **kwargs) -> ObjectId: ...
    @overload
    def __new__(self, related: TypeAlias, uselist=False, allow_none=False, *args, **kwargs) -> ObjectId: ...

class RelationProperty:
    @overload
    def __new__(self, related: TypeAlias, via: str=None, fetch=True) -> Any:...
    @overload
    def __new__(self, related: MC, via: str=None, fetch=True) -> MC:...


def __getattr__(name) -> Any: ...  # marks file as incomplete