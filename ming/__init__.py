import pymongo

from ming.session import Session
from ming.metadata import Field, Index, collection
from ming.declarative import Document
from ming.base import Cursor
from ming.version import __version__, __version_info__
from ming.config import configure
from ming.datastore import create_engine, create_datastore

# Re-export direction keys
ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING

