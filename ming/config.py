import six

from ming.exc import MingConfigError
from ming.session import Session
from ming.datastore import create_datastore


def variable_decode(**kwargs):
    try:
        from formencode.variabledecode import variable_decode as fe_variable_decode
    except ImportError:
        raise MingConfigError("Need to install FormEncode to use ``ming.configure``")
    return fe_variable_decode(kwargs)


def configure(**kwargs):
    """
    Given a (flat) dictionary of config values, creates DataStores
    and saves them by name
    """
    config = variable_decode(**kwargs)
    configure_from_nested_dict(config["ming"])


def configure_from_nested_dict(config):
    try:
        from formencode import schema, validators
    except ImportError:
        raise MingConfigError("Need to install FormEncode to use ``ming.configure``")

    class DatastoreSchema(schema.Schema):
        allow_extra_fields = True

        uri = validators.UnicodeString(if_missing=None, if_empty=None)
        database = validators.UnicodeString(if_missing=None, if_empty=None)
        connect_retry = validators.Number(if_missing=3, if_empty=0)
        auto_ensure_indexes = validators.StringBool(if_missing=True)
        # pymongo
        tz_aware = validators.Bool(if_missing=False)

    datastores = {}
    for name, datastore in six.iteritems(config):
        args = DatastoreSchema.to_python(datastore, None)
        database = args.pop("database", None)
        if database is None:
            datastores[name] = create_datastore(**args)
        else:
            datastores[name] = create_datastore(database=database, **args)
    Session._datastores = datastores
    # bind any existing sessions
    for name, session in six.iteritems(Session._registry):
        session.bind = datastores.get(name, None)
        session._name = name
