
from ming.exc import MingConfigError
from ming.session import Session
from ming.datastore import create_datastore, engine_key
from ming.tests import make_encryption_key


def variable_decode(**kwargs):
    try:
        from formencode.variabledecode import variable_decode as fe_variable_decode
    except ImportError:
        raise MingConfigError("Need to install FormEncode to use ``ming.configure``")
    return fe_variable_decode(kwargs)


def configure(share_engines=False, **kwargs):
    """
    Given a (flat) dictionary of config values, creates DataStores
    and saves them by name

    With ``share_engines=True``, datastores that connect to the same server
    with the same credentials and options share a single :class:`.Engine`
    (and so a single MongoClient and connection pool).
    """
    config = variable_decode(**kwargs)
    configure_from_nested_dict(config["ming"], share_engines=share_engines)


MIM_TEST_KEY = make_encryption_key('test local key')


def configure_from_nested_dict(config, share_engines=False):
    try:
        from formencode import schema, validators
        import ming.validators as ming_validators
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
        encryption = ming_validators.EncryptionConfigValidator(if_missing=None)

    datastores = {}
    engines = {}
    for name, datastore in config.items():
        if datastore['uri'].startswith('mim://') and 'encryption' not in datastore:
            datastore['encryption'] = {
                'kms_providers': {
                    'local': {
                        'key': MIM_TEST_KEY
                    }
                },
                'key_vault_namespace': 'encryption_test.coll_key_vault_test',
                'provider_options': {
                    'local': {
                        'key_alt_names': '["datakeyName"]'
                    }
                }
            }

        args = DatastoreSchema.to_python(datastore, None)
        database = args.pop("database", None)
        if database is None:
            ds = create_datastore(**args)
        else:
            ds = create_datastore(database=database, **args)
        if share_engines:
            key = engine_key(database=database, **args)
            if key is not None:
                # engines connect lazily, so the discarded one never opened a connection
                ds.bind = engines.setdefault(key, ds.bind)
        datastores[name] = ds
    Session._datastores = datastores
    # bind any existing sessions
    for name, session in Session._registry.items():
        session.bind = datastores.get(name, None)
        session._name = name
