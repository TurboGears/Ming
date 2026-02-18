from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar, Generic

from ming.utils import classproperty
import ming.schema as S

if TYPE_CHECKING:
    import ming.datastore
    from ming.metadata import Field
    from ming.odm.property import FieldProperty


# Suffix used to identify fields that should be encrypted
ENCRYPTED_SUFFIX = '_encrypted'


def _is_encrypted_field(field_name, field_type):
    """Check if a field should be encrypted based on naming convention.

    A field is encrypted if:
    1. Its name ends with '_encrypted'
    2. Its type is S.Binary

    :param field_name: The field name to check
    :param field_type: The schema type of the field
    :returns: True if the field should be encrypted
    """
    return field_name.endswith(ENCRYPTED_SUFFIX) and field_type is S.Binary


def _get_virtual_name(storage_name):
    """Get the user-facing name by stripping the _encrypted suffix.

    :param storage_name: The storage/schema field name (e.g., 'username_encrypted')
    :returns: The virtual name for user access (e.g., 'username')
    """
    if storage_name.endswith(ENCRYPTED_SUFFIX):
        return storage_name[:-len(ENCRYPTED_SUFFIX)]
    return storage_name


def _is_list_schema(schema_type):
    """Check if a schema type represents a list.

    :param schema_type: The schema type to check
    :returns: True if the schema represents a list
    """
    return isinstance(schema_type, list)


def _is_dict_schema(schema_type):
    """Check if a schema type represents a dict.

    :param schema_type: The schema type to check
    :returns: True if the schema represents a dict
    """
    return isinstance(schema_type, dict)


def _encrypt_value_recursive(value, schema, encr_func, field_name=None, force_encrypt=False):
    """Recursively encrypt a value according to its schema."""
    if value is None:
        return None

    if _is_dict_schema(schema):
        return _encrypt_dict_recursive(value, schema, encr_func)

    if _is_list_schema(schema):
        return _encrypt_list_recursive(value, schema, encr_func, field_name, force_encrypt=force_encrypt)

    if schema is S.Binary and (
        force_encrypt
        or (field_name and _is_encrypted_field(field_name, schema))
    ):
        return encr_func(value)

    return value


def _encrypt_dict_recursive(value, schema, encr_func):
    """Encrypt a dict value, handling virtual name mapping.

    :param value: The dict value to encrypt
    :param schema: The schema defining the dict structure
    :param encr_func: The encryption function to use
    :returns: A new dict with encrypted values and storage field names
    """
    if value is None:
        return None

    encrypted = {}
    processed_keys = set()

    # Process each field in the schema
    for storage_name, field_schema in schema.items():
        virtual_name = _get_virtual_name(storage_name)

        # Try to get value using virtual name first, then storage name
        source_value = None
        source_key = None
        if virtual_name in value:
            source_value = value[virtual_name]
            source_key = virtual_name
        elif storage_name in value:
            source_value = value[storage_name]
            source_key = storage_name

        if source_key is not None:
            processed_keys.add(source_key)
            encrypted[storage_name] = _encrypt_value_recursive(
                source_value, field_schema, encr_func, storage_name
            )

    # Copy non-schema fields as-is (fields not in the schema)
    for k, v in value.items():
        if k not in processed_keys:
            encrypted[k] = v

    return encrypted


def _encrypt_list_recursive(value, schema, encr_func, field_name, force_encrypt=False):
    """Encrypt a list value."""
    if not value:
        return value

    item_schema = schema[0] if schema else None
    item_field_name = field_name if field_name and field_name.endswith(ENCRYPTED_SUFFIX) else None
    items_encrypted = force_encrypt or bool(item_field_name)

    result = []
    for item in value:
        encrypted_value = _encrypt_value_recursive(
            item,
            item_schema,
            encr_func,
            field_name=item_field_name,
            force_encrypt=items_encrypted,
        )
        result.append(encrypted_value)

    return result


def _analyze_schema(schema):
    """Analyze a schema to extract field mappings and type information.

    :param schema: The schema dict to analyze
    :returns: A tuple of (virtual_to_storage, storage_to_virtual, encrypted_fields, nested_dicts, nested_lists)
    """
    virtual_to_storage = {}  # {'username': 'username_encrypted'}
    storage_to_virtual = {}  # {'username_encrypted': 'username'}
    encrypted_fields = set()  # {'username_encrypted'}
    nested_dicts = {}  # {'profile': {...schema...}}
    nested_lists = {}  # {'tags_encrypted': ([S.Binary], True)}

    for storage_name, field_type in schema.items():
        virtual_name = _get_virtual_name(storage_name)

        if virtual_name != storage_name:
            virtual_to_storage[virtual_name] = storage_name
            storage_to_virtual[storage_name] = virtual_name

        if _is_encrypted_field(storage_name, field_type):
            encrypted_fields.add(storage_name)
        elif _is_dict_schema(field_type):
            nested_dicts[storage_name] = field_type
        elif _is_list_schema(field_type):
            items_encrypted = storage_name.endswith(ENCRYPTED_SUFFIX)
            nested_lists[storage_name] = (field_type, items_encrypted)

    return virtual_to_storage, storage_to_virtual, encrypted_fields, nested_dicts, nested_lists


class EncryptedListWrapper:
    """List wrapper that transparently encrypts/decrypts list items.

    This class wraps a list and intercepts get/set operations to automatically
    decrypt values when reading and encrypt values when writing.

    Handles:
    - Lists of encrypted strings (when parent field has _encrypted suffix)
    - Lists of dicts with encrypted fields
    - Nested lists

    :param doc: The underlying list
    :param tracker: Ming's state tracker for dirty detection (optional, can be None for Document models)
    :param item_schema: Schema for list items (e.g., S.Binary, dict schema, or nested list)
    :param instance: Parent instance for encryption methods
    :param items_encrypted: True if list items themselves should be encrypted
    """

    def __init__(self, doc, tracker, item_schema, instance, items_encrypted=False):
        object.__setattr__(self, "_doc", doc)
        object.__setattr__(self, "_tracker", tracker)
        object.__setattr__(self, "_item_schema", item_schema)
        object.__setattr__(self, "_instance", instance)
        object.__setattr__(self, "_items_encrypted", items_encrypted)

    def _wrap_item(self, item):
        """Wrap or decrypt a single item for reading."""
        if item is None:
            return None

        if self._items_encrypted and self._item_schema is S.Binary:
            return self._instance.decr(item)
        elif _is_dict_schema(self._item_schema):
            return EncryptedDictWrapper(
                doc=item,
                tracker=self._tracker,
                schema=self._item_schema,
                instance=self._instance,
            )
        elif _is_list_schema(self._item_schema):
            nested_items_encrypted = self._items_encrypted
            nested_item_schema = self._item_schema[0] if self._item_schema else None
            return EncryptedListWrapper(
                doc=item,
                tracker=self._tracker,
                item_schema=nested_item_schema,
                instance=self._instance,
                items_encrypted=nested_items_encrypted,
            )
        return item

    def _encrypt_item(self, item):
        """Encrypt or process a single item for writing."""
        encrypted_iem = _encrypt_value_recursive(
            item,
            self._item_schema,
            self._instance.encr,
            field_name=ENCRYPTED_SUFFIX if self._items_encrypted else None,
            force_encrypt=self._items_encrypted,
        )
        return encrypted_iem

    def _mark_dirty(self):
        """Mark the list as modified for dirty tracking."""
        if self._tracker is not None:
            self._tracker.added_item(self._doc)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self._wrap_item(item) for item in self._doc[index]]
        return self._wrap_item(self._doc[index])

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            encrypted_values = [self._encrypt_item(item) for item in value]
            self._doc[index] = encrypted_values
        else:
            self._doc[index] = self._encrypt_item(value)
        self._mark_dirty()

    def __delitem__(self, index):
        del self._doc[index]
        self._mark_dirty()

    def __add__(self, other):
        return list(self) + list(other)

    def __radd__(self, other):
        return list(other) + list(self)

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __mul__(self, n):
        return list(self) * n

    def __rmul__(self, n):
        return n * list(self)

    def __imul__(self, n):
        if n <= 0:
            self.clear()
            return self

        current = list(self)
        for _ in range(n - 1):
            self.extend(current)
        return self

    def append(self, value):
        self._doc.append(self._encrypt_item(value))
        self._mark_dirty()

    def extend(self, values):
        for value in values:
            self._doc.append(self._encrypt_item(value))
        self._mark_dirty()

    def insert(self, index, value):
        self._doc.insert(index, self._encrypt_item(value))
        self._mark_dirty()

    def pop(self, index=-1):
        item = self._doc.pop(index)
        self._mark_dirty()
        return self._wrap_item(item)

    def remove(self, value):
        # Need to find and remove the encrypted version
        encrypted_value = self._encrypt_item(value)
        self._doc.remove(encrypted_value)
        self._mark_dirty()

    def index(self, value, *args):
        encrypted_value = self._encrypt_item(value)
        return self._doc.index(encrypted_value, *args)

    def replace(self, values):
        self[:] = values

    def clear(self):
        self._doc.clear()
        self._mark_dirty()

    def __iter__(self):
        for item in self._doc:
            yield self._wrap_item(item)

    def __len__(self):
        return len(self._doc)

    def __contains__(self, value):
        for item in self:
            if item == value:
                return True
        return False

    def __repr__(self):
        return f"EncryptedListWrapper({list(self)})"


class EncryptedDictWrapper:
    """Generic dict wrapper that transparently encrypts/decrypts specified fields.

    This class wraps the underlying document data and intercepts get/set operations
    to automatically decrypt values when reading and encrypt values when writing
    for fields with the _encrypted suffix convention.

    Supports:
    - Virtual name access (access 'username' but store 'username_encrypted')
    - Nested dicts with encrypted fields
    - Lists with encrypted items or encrypted fields in list items

    :param doc: The underlying document dict
    :param tracker: Ming's state tracker for dirty detection (optional, can be None for Document models)
    :param schema: The schema defining the field types
    :param instance: The parent instance (for encryption/decryption methods)
    """

    def __init__(self, doc, tracker, schema, instance):
        object.__setattr__(self, "_doc", doc)
        object.__setattr__(self, "_tracker", tracker)
        object.__setattr__(self, "_schema", schema)
        object.__setattr__(self, "_instance", instance)

        # Analyze schema to build mappings
        (virtual_to_storage, storage_to_virtual, encrypted_fields,
         nested_dicts, nested_lists) = _analyze_schema(schema)

        object.__setattr__(self, "_virtual_to_storage", virtual_to_storage)
        object.__setattr__(self, "_storage_to_virtual", storage_to_virtual)
        object.__setattr__(self, "_encrypted_fields", encrypted_fields)
        object.__setattr__(self, "_nested_dicts", nested_dicts)
        object.__setattr__(self, "_nested_lists", nested_lists)

    def _resolve_key(self, key):
        """Resolve a key to its storage name.

        Accepts both virtual names ('username') and storage names ('username_encrypted').
        Returns the storage name.
        """
        if key in self._virtual_to_storage:
            return self._virtual_to_storage[key]
        return key

    def _get_virtual_key(self, storage_key):
        """Get the virtual name for a storage key."""
        return self._storage_to_virtual.get(storage_key, storage_key)

    def _mark_dirty(self, value):
        """Mark the value as modified for dirty tracking."""
        if self._tracker is not None:
            self._tracker.added_item(value)

    def _wrap_storage_value(self, storage_key, value):
        if storage_key in self._encrypted_fields and value is not None:
            return self._instance.decr(value)
        if storage_key in self._nested_dicts and value is not None:
            return EncryptedDictWrapper(
                doc=value,
                tracker=self._tracker,
                schema=self._nested_dicts[storage_key],
                instance=self._instance,
            )
        if storage_key in self._nested_lists and value is not None:
            list_schema, items_encrypted = self._nested_lists[storage_key]
            item_schema = list_schema[0] if list_schema else None
            return EncryptedListWrapper(
                doc=value,
                tracker=self._tracker,
                item_schema=item_schema,
                instance=self._instance,
                items_encrypted=items_encrypted,
            )
        return value

    def __getitem__(self, key):
        storage_key = self._resolve_key(key)
        value = self._doc[storage_key]
        return self._wrap_storage_value(storage_key, value)

    def __setitem__(self, key, value):
        storage_key = self._resolve_key(key)

        if storage_key in self._encrypted_fields and value is not None:
            value = self._instance.encr(value)
        elif storage_key in self._nested_dicts and value is not None:
            value = _encrypt_dict_recursive(value, self._nested_dicts[storage_key], self._instance.encr)
        elif storage_key in self._nested_lists and value is not None:
            list_schema, items_encrypted = self._nested_lists[storage_key]
            value = _encrypt_list_recursive(
                value,
                list_schema,
                self._instance.encr,
                storage_key if items_encrypted else None,
                force_encrypt=items_encrypted,
            )

        old_value = self._doc.get(storage_key)
        if old_value != value:
            self._doc[storage_key] = value
            self._mark_dirty(value)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        if key.startswith('_') or hasattr(self.__class__, key):
            object.__setattr__(self, key, value)
            return
        self[key] = value

    def __contains__(self, key):
        storage_key = self._resolve_key(key)
        return storage_key in self._doc

    def __delitem__(self, key):
        storage_key = self._resolve_key(key)
        value = self._doc.pop(storage_key)
        self._mark_dirty(value)

    def pop(self, key, *default):
        if len(default) > 1:
            raise TypeError('pop expected at most 2 arguments')

        storage_key = self._resolve_key(key)
        if storage_key not in self._doc:
            if default:
                return default[0]
            raise KeyError(key)

        value = self._doc.pop(storage_key)
        self._mark_dirty(value)
        return self._wrap_storage_value(storage_key, value)

    def popitem(self):
        storage_key, value = self._doc.popitem()
        self._mark_dirty(value)
        return self._get_virtual_key(storage_key), self._wrap_storage_value(storage_key, value)

    def clear(self):
        if self._doc:
            self._doc.clear()
            self._mark_dirty(None)

    def setdefault(self, key, default=None):
        if key in self:
            return self[key]
        self[key] = default
        return self[key]

    def update(self, *args, **kwargs):
        if len(args) > 1:
            raise TypeError('update expected at most 1 arguments')

        if args:
            source = args[0]
            if hasattr(source, 'keys'):
                for key in source:
                    self[key] = source[key]
            else:
                for key, value in source:
                    self[key] = value

        for key, value in kwargs.items():
            self[key] = value

    def replace(self, value):
        self.clear()
        self.update(value)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        """Return virtual names for keys."""
        return (self._get_virtual_key(k) for k in self._doc.keys())

    def values(self):
        return (self[self._get_virtual_key(k)] for k in self._doc)

    def items(self):
        return ((self._get_virtual_key(k), self[self._get_virtual_key(k)]) for k in self._doc)

    def __iter__(self):
        """Iterate over virtual names."""
        return (self._get_virtual_key(k) for k in self._doc)

    def __len__(self):
        return len(self._doc)

    def __eq__(self, other):
        """Compare with another dict or EncryptedDictWrapper."""
        if isinstance(other, EncryptedDictWrapper):
            return dict(self.items()) == dict(other.items())
        elif isinstance(other, dict):
            return dict(self.items()) == other
        return NotImplemented

    def __repr__(self):
        return f"EncryptedDictWrapper({dict(self.items())})"


class MingEncryptionError(Exception):
    pass


class EncryptionConfig:
    """
    A class to hold the encryption configuration for a ming datastore.

    :param config: a dictionary that closely resembles various features of the MongoDB
        encryption that we support.
    """
    
    def __init__(self, config: dict):
        self._encryption_config = config

    @property
    def kms_providers(self) -> dict:
        """
        Returns the kms providers used in this configuration. These values are passed directly to pymongo.

        See the documentation for the :class:`pymongo.encryption.ClientEncryption` constructor
        for more information on valid values for kms_providers.

        A typical example of the kms_providers field using the `local` provider would look like this:
        
        .. :code-block: json
            
                {
                    "local": {   
                        "key": "<base64-encoded-key>",
                    }
                }

        """
        return self._encryption_config.get('kms_providers')

    @property
    def provider_options(self) -> dict:
        """
        Returns all of the provider options used by this configuration when calling the underlying 
        :meth:`pymongo.encryption.ClientEncryption.create_data_key` method.

        See the documentation for pymongo's :meth:`pymongo.encryption.ClientEncryption.create_data_key`
        method for more information on valid values for ``provider_options``.

        A typical example of the ``provider_options`` field using the ``local`` provider would look like this:
        
        .. :code-block: json
            
                {
                    "local": {   
                        "key_alt_names": ["datakey_test1", "datakey_test2"]
                    },
                    "gcp": { ... },
                    ...
                }

        """
        return self._encryption_config.get('provider_options')

    def _get_key_alt_name(self, provider='local') -> str:
        return self.provider_options.get(provider)['key_alt_names'][0]

    @property
    def key_vault_namespace(self) -> str:
        """Describes which mongodb database/collection combo your auto-generated 
        encryption data keys will be stored.

        This is a string in the format ``<database>.<collection>``.
        """
        return self._encryption_config.get('key_vault_namespace')


T = TypeVar('T')


class DecryptedField(Generic[T]):

    def __init__(self, field_type: type[T], encrypted_field: str):
        """
        Creates a field that acts as an automatic getter/setter for the target
        field name specified ``encrypted_field``.

        .. note::

            Interally :class:``.DecryptedField`` uses getattr and setattr on ``self`` using the ``encrypted_field`` name.

        .. code-block:: python

            class MyDocument(Document):
                email_encrypted = Field(ming.schema.Binary)
                email = DecryptedField(str, 'email_encrypted')

        :param field_type: The Type of the decrypted field
        :param encrypted_field: The name of the encrypted attribute to operate on
        """
        self.field_type = field_type
        self.encrypted_field = encrypted_field

    def __get__(self, instance: EncryptedMixin, owner) -> T:
        if instance is None:
            return self
        return instance.decr(getattr(instance, self.encrypted_field))

    def __set__(self, instance: EncryptedMixin, value: T):
        # allow None, because most normal fields do not have required=True set, nor the (undocumented) allow_none
        if value is not None and not isinstance(value, self.field_type):
            raise TypeError(f'not {self.field_type}, got {value!r}')
        setattr(instance, self.encrypted_field, instance.encr(value))


class EncryptedMixin:
    """A mixin intended to be used with :class:`~ming.declarative.Document`
    or :class:`~ming.odm.declarative.MappedClass` to provide encryption.
    All configuration is handled by an instance of a :class:`ming.encryption.EncryptionConfig`
    that is passed to the :class:`ming.datastore.DataStore` instance that the Document/MappedClass is bound to.

    Generally, don't use this directly, but instead call the methods on the Document/MappedClass you're working with.
    """

    @classproperty
    def _datastore(cls) -> ming.datastore.DataStore:
        from ming.declarative import Document
        from ming.odm.declarative import MappedClass
        if issubclass(cls, Document):
            return cls.m.session.bind
        if issubclass(cls, MappedClass):
            return cls.query.session.bind
        raise NotImplementedError("Unexpected class type. You must implement `datastore` as a @classproperty in your mixin implementation.")
    
    @classproperty
    def _field_names(cls) -> list[str]:
        from ming.declarative import Document
        from ming.odm.declarative import MappedClass
        if issubclass(cls, Document):
            fields: list[tuple[str, Field]] = list(cls.m.field_index.items())
            field_names = []
            for (k, v) in fields:
                if v.type in (S.Deprecated,):
                    continue
                field_names.append(k)
            return field_names
        if issubclass(cls, MappedClass):
            fields: list[tuple[str, FieldProperty]] = list(cls.query.mapper.property_index.items())
            field_names = []
            for (k, v) in fields:
                if v.field.type in (S.Deprecated,):
                    continue
                field_names.append(k)
            return field_names
        raise NotImplementedError("Unexpected class type. You must implement `field_names` as a @classproperty in your mixin implementation.")

    @classmethod
    def encr(cls, s: str | None, provider='local') -> bytes | None:
        """Encrypts a string using the encryption configuration of the ming datastore that this class is bound to.
        Most of the time, you won't need to call this directly, as it is used by the :meth:`ming.encryption.EncryptedDocumentMixin.encrypt_some_fields` method.
        """
        return cls._datastore.encr(s, provider=provider)

    @classmethod
    def decr(cls, b: bytes | None) -> str | None:
        """Decrypts a string using the encryption configuration of the ming datastore that this class is bound to.
        """
        return cls._datastore.decr(b)

    @classmethod
    def decrypted_field_names(cls) -> list[str]:
        """
        Returns a list of field names that have ``_encrypted`` counterts.

        For example, if a class has fields ``email`` and ``email_encrypted``, this method would return ``['email']``.
        """
        return [fld.replace('_encrypted', '')
                for fld in cls.encrypted_field_names()]

    @classmethod
    def encrypted_field_names(cls) -> list[str]:
        """
        Returns the field names of all encrypted fields. Fields are assumed to be encrypted if they end with ``_encrypted``.

        For example if a class has fields ``email`` and ``email_encrypted``, this method would return ``['email_encrypted']``.
        """
        return [fld for fld in cls._field_names
                if fld.endswith('_encrypted')]

    @classmethod
    def _encrypted_field_index(cls) -> dict[str, object]:
        """Return nested-encryption declarations keyed by field name."""
        from ming.declarative import Document
        from ming.odm.declarative import MappedClass
        if issubclass(cls, Document):
            return {
                name: field for name, field in cls.m.field_index.items()
                if isinstance(field, NestedEncryptedField)
            }
        if issubclass(cls, MappedClass):
            return {
                name: prop for name, prop in cls.query.mapper.property_index.items()
                if isinstance(prop, NestedEncryptedProperty)
            }
        return {}

    @classmethod
    def encrypt_some_fields(cls, data: dict) -> dict:
        """Encrypts some fields in a dictionary using the encryption configuration of the ming datastore that this class is bound to.

        This handles both:
        1. Top-level encrypted fields (e.g., ``email`` → ``email_encrypted``)
        2. Nested encrypted fields in NestedEncryptedField/NestedEncryptedProperty schemas
           (e.g., ``author.username`` → ``author.username_encrypted``)

        :param data: a dictionary of data to be encrypted
        :return: a modified copy of the ``data`` param with the currently-unencrypted-but-encryptable fields replaced with ``_encrypted`` counterparts.
        """
        encrypted_data = data.copy()

        # Handle top-level encrypted fields (e.g., email -> email_encrypted)
        for fld in cls.decrypted_field_names():
            if fld in encrypted_data:
                val = encrypted_data.pop(fld)
                encrypted_data[f'{fld}_encrypted'] = cls.encr(val)

        # Handle nested encrypted field/property instances.
        for field_name, field in cls._encrypted_field_index().items():
            if field_name in encrypted_data:
                val = encrypted_data[field_name]
                if val is not None:
                    schema = field._encrypted_schema
                    if field._is_list:
                        encrypted_data[field_name] = _encrypt_list_recursive(val, schema, cls.encr, field_name,
                                                                             force_encrypt=True)
                    elif field._is_dict:
                        encrypted_data[field_name] = _encrypt_dict_recursive(val, schema, cls.encr)

        return encrypted_data

    def decrypt_some_fields(self) -> dict:
        """
        Returns a `dict` with raw data. Removes encrypted fields and replaces them with decrypted data. Useful for json.
        """
        decrypted_data = dict()
        for k in self._field_names:
            if k.endswith('_encrypted'):
                k_decrypted = k.replace('_encrypted', '')
                decrypted_data[k_decrypted] = getattr(self, k_decrypted)
            else:
                decrypted_data[k] = getattr(self, k)
        return decrypted_data


class NestedEncryptedField:
    """Field declaration for nested dict/list schemas with encrypted leaves."""

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if isinstance(args[0], str):
                raise ValueError(
                    'When called with only one argument NestedEncryptedField() '
                    'parameter should be the field type'
                )
            self.name = None
            self.type = args[0]
        elif len(args) == 2:
            self.name = args[0]
            self.type = args[1]
        else:
            raise TypeError(
                'NestedEncryptedField() takes 1 or 2 arguments, not %s' % len(args)
            )

        self.schema = S.SchemaItem.make(self.type, **kwargs)
        self._encrypted_schema = self.type
        self._is_list = _is_list_schema(self.type)
        self._is_dict = _is_dict_schema(self.type)

        if not (self._is_list or self._is_dict):
            raise TypeError('NestedEncryptedField requires a dict or list schema')

    def __repr__(self):
        return f'<NestedEncryptedField {self.name}({self.schema})>'


class NestedEncryptedFieldDescriptor:
    """Descriptor for NestedEncryptedField."""

    def __init__(self, field: NestedEncryptedField):
        self.field = field
        self.name = field.name

    def __get__(self, inst, cls=None):
        if inst is None:
            return self

        schema = self.field._encrypted_schema
        if self.field._is_list:
            doc = inst.get(self.name)
            if doc is None:
                doc = []
                inst[self.name] = doc
            item_schema = schema[0] if schema else None
            return EncryptedListWrapper(
                doc=doc,
                tracker=None,
                item_schema=item_schema,
                instance=inst,
                items_encrypted=True,
            )

        doc = inst.get(self.name)
        if doc is None:
            doc = {}
            inst[self.name] = doc
        return EncryptedDictWrapper(
            doc=doc,
            tracker=None,
            schema=schema,
            instance=inst,
        )

    def __set__(self, inst, value):
        if value is None:
            inst[self.name] = value
            return

        schema = self.field._encrypted_schema
        if self.field._is_list:
            encrypted_value = _encrypt_list_recursive(value, schema, inst.encr, self.name, force_encrypt=True)
        else:
            encrypted_value = _encrypt_dict_recursive(value, schema, inst.encr)

        inst[self.name] = encrypted_value

    def __delete__(self, inst):
        del inst[self.name]


class NestedEncryptedProperty:
    """FieldProperty declaration for nested dict/list schemas with encrypted leaves."""

    _impl_class = None

    @classmethod
    def _get_impl_class(cls):
        if cls._impl_class is None:
            from ming.odm.property import FieldProperty

            # Avoid importing FieldProperty at module import time to prevent
            # ming.encryption <-> ming.odm.property circular import issues.
            class _NestedEncryptedPropertyImpl(NestedEncryptedProperty, FieldProperty):
                pass

            cls._impl_class = _NestedEncryptedPropertyImpl
        return cls._impl_class

    def __new__(cls, *args, **kwargs):
        if cls is NestedEncryptedProperty:
            return object.__new__(cls._get_impl_class())
        return object.__new__(cls)

    def __init__(self, schema):
        self.schema = schema
        self._encrypted_schema = schema
        self._is_list = _is_list_schema(schema)
        self._is_dict = _is_dict_schema(schema)

        if not (self._is_list or self._is_dict):
            raise TypeError('NestedEncryptedProperty requires a dict or list schema')

        super().__init__(schema)

    def __set__(self, instance, value):
        if value is None:
            super().__set__(instance, value)
            return

        if self._is_list:
            encrypted_value = _encrypt_list_recursive(value, self.schema, instance.encr, self.name, force_encrypt=True)
        else:
            encrypted_value = _encrypt_dict_recursive(value, self.schema, instance.encr)

        super().__set__(instance, encrypted_value)

    def __get__(self, instance, cls=None):
        if instance is None:
            return self

        from ming.odm.base import state

        st = state(instance)
        document = st.document if st.document is not None else {}

        if self._is_list:
            doc = document.get(self.name)
            if doc is None:
                doc = []
                document[self.name] = doc
            item_schema = self.schema[0] if self.schema else None
            return EncryptedListWrapper(
                doc=doc,
                tracker=st.tracker,
                item_schema=item_schema,
                instance=instance,
                items_encrypted=True,
            )

        doc = document.get(self.name)
        if doc is None:
            doc = {}
            document[self.name] = doc
        return EncryptedDictWrapper(
            doc=doc,
            tracker=st.tracker,
            schema=self.schema,
            instance=instance,
        )
