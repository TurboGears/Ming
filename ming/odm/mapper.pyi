from typing import type_check_only, TypeVar, Generic, Optional, Union, Any, Dict, overload

from bson import ObjectId
from pymongo.command_cursor import CommandCursor

from ming.base import Cursor

M = TypeVar('M')

MongoFilter = dict
ChangeResult = dict
class _ClassQuery(Generic[M]):
    # proxies most of these from Session
    def get(self, _id: Union[ObjectId|Any] = None, **kwargs) -> Optional[M]: ...
    def find(self, filter: MongoFilter = None, *args, **kwargs) -> Cursor[M]: ...
    def find_by(self, filter: MongoFilter = None, *args, **kwargs) -> Cursor[M]: ...
    def remove(self, spec_or_id: Union[MongoFilter, ObjectId] = None, **kwargs) -> ChangeResult: ...
    def count(self) -> int: ...
    def find_one_and_update(self, **kwargs) -> M: ...
    def find_one_and_replace(self, **kwargs) -> M: ...
    def find_one_and_delete(self, **kwargs) -> M: ...
    def update_partial(self, filter: MongoFilter, fields: dict, **kwargs) -> ChangeResult: ...
    def aggregate(self, pipeline: list, **kwargs) -> CommandCursor: ...
    def distinct(self, key: str, filter: MongoFilter | None = None, **kwargs) -> list: ...

class _InstQuery(object):
    # proxied from session:
    def update_if_not_modified(self, obj, fields, upsert=False) -> bool: ...

    def delete(self) -> ChangeResult: ...

@type_check_only
class Query(_ClassQuery[M], _InstQuery):

    @overload  # from _ClassQuery
    def update(self, spec: MongoFilter, fields: dict, **kwargs) -> ChangeResult: ...

    @overload  # from _InstQuery
    def update(self, fields, **kwargs) -> ChangeResult: ...


def __getattr__(name) -> Any: ...  # marks file as incomplete
