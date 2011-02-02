import pymongo

from session import Session
from base import Document, Field
from version import __version__, __version_info__

# Re-export direction keys
ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING

def configure(**kwargs):
    """
    Given a dictionary of config values, creates DataStores and saves them by name
    """
    from datastore import DataStore
    from formencode.variabledecode import variable_decode
    from formencode import schema, validators

    class DatastoreSchema(schema.Schema):
        master=validators.UnicodeString(if_missing=None, if_empty=None)
        slave=validators.UnicodeString(if_missing=None, if_empty=None)
        database=validators.UnicodeString(not_empty=True)
        connect_retry=validators.Number(if_missing=3, if_empty=0)
        # pymongo
        network_timeout=validators.Number(if_missing=None, if_empty=None)
        tz_aware=validators.Bool(if_missing=False)

    config = variable_decode(kwargs)
    datastores = {}
    for name, datastore in config['ming'].iteritems():
        args = DatastoreSchema.to_python(datastore)
        datastores[name] = DataStore(**args)
    Session._datastores = datastores
    # bind any existing sessions
    for name, session in Session._registry.iteritems():
        session.bind = datastores.get(name, None)
