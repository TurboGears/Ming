from typing import Generator, List, type_check_only, TypeVar, Generic, Optional, Union, Any, overload, Type

from bson import ObjectId
from pymongo.command_cursor import CommandCursor
import pymongo.results


from ming import Document, Cursor, Session
from ming.base import Object
from ming.odm import MappedClass, FieldProperty, ODMSession
from ming.odm.base import ObjectState

TMappedClass = TypeVar('M', bound=MappedClass)
TDocument = TypeVar('D', bound=Document)

def mapper(cls: Type[TMappedClass], collection: TDocument = None, session: Session = None, **kwargs) -> Mapper: ...

class Mapper(Generic[TMappedClass]):

    properties: List[FieldProperty]
    property_index: dict[str, FieldProperty]
    collection: Type[TDocument]
    mapped_class: Type[TMappedClass]
    session: Session
    extensions: list
    options: Object

    def __init__(self, mapped_class: Type[TMappedClass], collection: Type[Document], session: Session, **kwargs): ...

    @classmethod
    def replace_session(cls, session) -> None: ...

    def insert(self, obj: MappedClass, state: ObjectState, session: ODMSession, **kwargs) -> pymongo.results.InsertOneResult: ...

    def update(self, obj: MappedClass, state: ObjectState, session: ODMSession, **kwargs) -> ObjectId: ...

    def delete(self, obj: MappedClass, state: ObjectState, session: ODMSession, **kwargs) -> pymongo.results.DeleteResult: ...

    def remove(self, session: ODMSession, *args, **kwargs) -> pymongo.results.DeleteResult: ...

    def create(self, doc, options, remake=True) -> TMappedClass: ...

    def base_mappers(self) -> Generator[Mapper]: ...

    def all_properties(self) -> Generator[FieldProperty]: ...

    @classmethod
    def by_collection(cls, collection_class: Type[TDocument]) -> Mapper[TMappedClass]: ...

    @classmethod
    def by_class(cls, mapped_class: Type[TMappedClass]) -> Mapper[TMappedClass]: ...

    @classmethod
    def by_classname(cls, name: str) -> Mapper[TMappedClass]: ...

    @classmethod
    def all_mappers(cls) -> List[Mapper[TMappedClass]]: ...

    @classmethod
    def compile_all(cls): ...

    @classmethod
    def clear_all(cls): ...

    @classmethod
    def ensure_all_indexes(cls): ...

    def compile(self): ...

    def update_partial(self, session: ODMSession, *args, **kwargs) -> pymongo.results.UpdateResult: ...


MongoFilter = dict
ChangeResult = dict
class _ClassQuery(Generic[TMappedClass]):
    # proxies most of these from Session
    def get(self, _id: Union[ObjectId|Any] = None, **kwargs) -> Optional[TMappedClass]: ...
    def find(self, filter: MongoFilter = None, *args, **kwargs) -> Cursor[TMappedClass]: ...
    def find_by(self, filter: MongoFilter = None, *args, **kwargs) -> Cursor[TMappedClass]: ...
    def remove(self, spec_or_id: Union[MongoFilter, ObjectId] = None, **kwargs) -> ChangeResult: ...
    def count(self) -> int: ...
    def find_one_and_update(self, filter: MongoFilter, update: dict, **kwargs) -> TMappedClass: ...
    def find_one_and_replace(self, filter: MongoFilter, replacement: dict, *args, **kwargs) -> TMappedClass: ...
    def find_one_and_delete(self, filter: MongoFilter, **kwargs) -> TMappedClass: ...
    def update_partial(self, filter: MongoFilter, fields: dict, **kwargs) -> ChangeResult: ...
    def aggregate(self, pipeline: list, **kwargs) -> CommandCursor: ...
    def distinct(self, key: str, filter: MongoFilter | None = None, **kwargs) -> list: ...

class _InstQuery(object):
    # proxied from session:
    def update_if_not_modified(self, obj, fields, upsert=False) -> bool: ...

    def delete(self) -> ChangeResult: ...

@type_check_only
class Query(_ClassQuery[TMappedClass], _InstQuery):

    @overload  # from _ClassQuery
    def update(self, spec: MongoFilter, fields: dict, **kwargs) -> ChangeResult: ...

    @overload  # from _InstQuery
    def update(self, fields, **kwargs) -> ChangeResult: ...


def __getattr__(name) -> Any: ...  # marks file as incomplete
