from configparser import ConfigParser
import os
from unittest import SkipTest, TestCase

import ming
from ming import create_datastore, Document, Field, schema as S
from ming.odm import session, ODMSession, Mapper, MappedClass, FieldProperty, DecryptedProperty
from ming.encryption import DecryptedField, EncryptedField
from ming.odm.odmsession import ThreadLocalODMSession

from . import make_encryption_key

InvalidClass = Exception

def import_formencode():
    try:
        from formencode import Invalid
        global InvalidClass
        InvalidClass = Invalid
    except ImportError:
        raise SkipTest("Need to install FormEncode to use ``ming.configure``")


class TestEncryptionConfig(TestCase):

    LOCAL_KEY = make_encryption_key('test local key')

    def setUp(self):
        import_formencode()

    def _parse_config(self, body: str):
        config_lines = [
            l.strip() for l in 
            [
                '[app:main]',
            ] + body.split('\n')
            if l
        ]

        config = ConfigParser()
        config.read_string('\n'.join(config_lines))
        ming.configure(**config['app:main'])
        return config

    def test_validation_good(self):
        config_str = f"""
            ming.maindb.uri = mim://host/maindb
            ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
            ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
            ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1
        """
        
        self._parse_config(config_str)
        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertIsNotNone(encryption)
        self.assertEqual(encryption.kms_providers, {'local': {'key': self.LOCAL_KEY}})
        self.assertEqual(encryption.key_vault_namespace, 'encryption_test.dataKeyVault')
        self.assertEqual(encryption.provider_options, {'local': {'key_alt_names': ['datakey_test1']}})

    def test_validation_key_alt_names(self):
        config_str = f"""
            ming.maindb.uri = mim://host/maindb
            ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
            ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
            ming.maindb.encryption.provider_options.local.key_alt_names = ["datakey_test1"]
        """
        
        self._parse_config(config_str)
        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertEqual(encryption.provider_options, {'local': {'key_alt_names': ['datakey_test1']}})

    def test_validation_key_alt_names2(self):
        config_str = f"""
            ming.maindb.uri = mim://host/maindb
            ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
            ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
            ming.maindb.encryption.provider_options.local.key_alt_names = ["datakey_test1", "datakey_test2"]
        """
        
        self._parse_config(config_str)
        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertEqual(encryption.provider_options, {'local': {'key_alt_names': ['datakey_test1', 'datakey_test2']}})

    def test_validation_key_alt_names3(self):
        config_str = f"""
            ming.maindb.uri = mim://host/maindb
            ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
            ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
            ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1, datakey_test2
        """
        
        self._parse_config(config_str)
        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertEqual(encryption.provider_options, {'local': {'key_alt_names': ['datakey_test1', 'datakey_test2']}})

    def test_validation_key_alt_names4(self):
        config_str = f"""
            ming.maindb.uri = mim://host/maindb
            ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
            ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
            ming.maindb.encryption.provider_options.local.key_alt_names = "datakey_test1", "datakey_test2"
        """
        
        self._parse_config(config_str)
        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertEqual(encryption.provider_options, {'local': {'key_alt_names': ['datakey_test1', 'datakey_test2']}})

    def test_validation_empty(self):
        self._parse_config(f'''ming.maindb.uri = mongo://host/maindb''')

        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertIsNone(encryption)

    def test_validation_empty_mim_auto_encryption(self):
        self._parse_config(f'''ming.maindb.uri = mim://host/maindb''')

        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertIsNotNone(encryption.kms_providers['local']['key'])
        self.assertEqual(encryption.key_vault_namespace, 'encryption_test.coll_key_vault_test')
        self.assertEqual(encryption.provider_options, {'local': {'key_alt_names': ['datakeyName']}})

    def test_validation_bad_missing(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['key_vault_namespace', 'provider_options']), set(error_dict.keys()))
        self.assertIn('Missing required encryption configuration field ', str(error_dict['key_vault_namespace']))
        self.assertIn('Missing required encryption configuration field ', str(error_dict['provider_options']))

    def test_validation_bad_missing2(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['kms_providers', 'provider_options']), set(error_dict.keys()))
        self.assertIn('Missing required encryption configuration field ', str(error_dict['kms_providers']))
        self.assertIn('Missing required encryption configuration field ', str(error_dict['provider_options']))

    def test_validation_bad_missing3(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['kms_providers', 'key_vault_namespace']), set(error_dict.keys()))
        self.assertIn('Missing required encryption configuration field ', str(error_dict['kms_providers']))
        self.assertIn('Missing required encryption configuration field ', str(error_dict['key_vault_namespace']))

    def test_validation_bad_extra(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.extra_nonsense = foo
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['extra_nonsense']), set(error_dict.keys()))
        self.assertIn("Unexpected encryption configuration field 'extra_nonsense'", str(error_dict['extra_nonsense']))

    def test_validation_bad_empty(self):
        self._parse_config(f'''
            ming.maindb.uri = mim://host/maindb
            ming.maindb.encryption.kms_providers = 
            ming.maindb.encryption.key_vault_namespace = 
            ming.maindb.encryption.provider_options = 
        ''')
        encryption = ming.Session.by_name('maindb').bind.encryption
        self.assertEqual(encryption.kms_providers, '')
        self.assertEqual(encryption.provider_options, '')
        self.assertEqual(encryption.provider_options, '')

    def test_validation_bad_kms_providers(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.kms_providers.BAD.key = {self.LOCAL_KEY}
                ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
                ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['kms_providers']), set(error_dict.keys()))
        self.assertIn("Invalid kms_provider(s)", str(error_dict['kms_providers']))

    def test_validation_bad_kms_provider_local(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.kms_providers.local.foo = {self.LOCAL_KEY}
                ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
                ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['kms_providers']), set(error_dict.keys()))
        self.assertIn("kms_provider 'local' requires", str(error_dict['kms_providers']))

    def test_validation_bad_provider_options_local(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
                ming.maindb.encryption.key_vault_namespace = encryption_test.dataKeyVault
                ming.maindb.encryption.provider_options.local.foo = bar'
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['provider_options']), set(error_dict.keys()))
        self.assertIn('requires provider_options', str(error_dict['provider_options']))

    def test_validation_bad_key_vault_namespace(self):
        with self.assertRaises(InvalidClass) as e:
            self._parse_config(f'''
                ming.maindb.uri = mim://host/maindb
                ming.maindb.encryption.kms_providers.local.key = {self.LOCAL_KEY}
                ming.maindb.encryption.key_vault_namespace = encryption_test_dataKeyVault
                ming.maindb.encryption.provider_options.local.key_alt_names = datakey_test1
            ''')
        error_dict = e.exception.error_dict['encryption'].error_dict
        self.assertEqual(set(['key_vault_namespace']), set(error_dict.keys()))
        self.assertIn('Invalid key_vault_namespace', str(error_dict['key_vault_namespace']))


class TestDocumentEncryption(TestCase):
    DATASTORE = "mim://host/test_db"

    def setUp(self):
        import_formencode()

        ming.configure(**{
            'ming.test_db.uri': self.DATASTORE,
            'ming.test_db.encryption.kms_providers.local.key': make_encryption_key(self.__class__.__name__),
            'ming.test_db.encryption.key_vault_namespace': 'encryption_test.coll_key_vault_test',
            'ming.test_db.encryption.provider_options.local.key_alt_names': '["test_datakey_1"]'
        })

    def tearDown(self):
        session = ming.Session.by_name('test_db')
        session.bind.conn.drop_database('test_db')
        session.bind.conn.drop_database('encryption_test')

    def test_document(self):
        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = ming.Session.by_name('test_db')
                indexes = [ ('name_encrypted',) ]
            _id = Field(S.Anything)
            name = DecryptedField(str, 'name_encrypted')
            name_encrypted = Field(S.Binary)
            other = Field(str)
            deprecated = FieldProperty(S.Deprecated)

        doc = TestDoc.make_encr(dict(_id=1, name='Jerome', other='foo'))
        doc.m.save()

        self.assertEqual(doc.name, 'Jerome')
        self.assertIsInstance(doc.name, str)
        self.assertIsInstance(doc.name_encrypted, bytes)
        self.assertEqual(doc.name_encrypted, TestDoc.encr('Jerome'))
        self.assertEqual(doc.name, TestDoc.decr(doc.name_encrypted))

        doc.name = 'Jessie'
        doc.m.save()
        self.assertEqual(doc.name, 'Jessie')
        self.assertEqual(doc.name_encrypted, TestDoc.encr('Jessie'))
        self.assertEqual(doc.name, TestDoc.decr(doc.name_encrypted))

        self.assertEqual(doc.decrypt_some_fields(), {'_id': 1, 'name': 'Jessie', 'other': 'foo'})
        self.assertIsNotNone(TestDoc.m.get(name_encrypted=TestDoc.encr('Jessie')))

        self.assertEqual(set(doc.encrypted_field_names()), set(['name_encrypted']))
        self.assertEqual(set(doc.decrypted_field_names()), set(['name']))
        self.assertEqual(set(doc._field_names), set(['_id', 'name_encrypted', 'other']))

        # allowed to save None to it
        doc.name = None
        doc.m.save()
        self.assertEqual(doc.name, None)
        self.assertEqual(doc.name_encrypted, None)

class TestDocumentEncryptionMimAutoSettings(TestDocumentEncryption):
    def setUp(self):
        # replace super() NOT using it
        import_formencode()

        ming.configure(**{
            'ming.test_db.uri': self.DATASTORE,
            # mim encryption settings should come automatically from configure_from_nested_dict
        })

class TestDocumentEncryptionReal(TestDocumentEncryption):
    DATASTORE = f"mongodb://localhost/test_ming_TestDocumentReal_{os.getpid()}?serverSelectionTimeoutMS=100"

class TestMapping(TestCase):
    DATASTORE = 'mim:///test_db'

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        ming.configure(**{
            'ming.test_db.uri': self.DATASTORE,
            'ming.test_db.encryption.kms_providers.local.key': make_encryption_key(self.__class__.__name__),
            'ming.test_db.encryption.key_vault_namespace': 'encryption_test.coll_key_vault_test',
            'ming.test_db.encryption.provider_options.local.key_alt_names': '["test_datakey_1"]'
        })
        # self.datastore = create_datastore(self.DATASTORE)
        self.datastore = ming.Session._datastores.get('test_db')
        self.session = ODMSession(bind=self.datastore)

    def tearDown(self):
        self.session.clear()
        try:
            self.datastore.conn.drop_all()
        except TypeError:
            self.datastore.conn.drop_database(self.datastore.db)
            self.datastore.conn.drop_database('encryption_test')
        Mapper._mapper_by_classname.clear()

    def test(self):
        class TestMapped(MappedClass):
            class __mongometa__:
                name = "test_mapped"
                session = self.session

            _id = FieldProperty(S.ObjectId)
            name = DecryptedProperty(str, 'name_encrypted')
            name_encrypted = FieldProperty(S.Binary)
            other = FieldProperty(str)
            deprecated = FieldProperty(S.Deprecated)

        u = TestMapped(_id=None, name="Jerome", other='foo')
        self.session.flush()
        self.assertEqual(u.decrypt_some_fields(), {'_id': None, 'name': 'Jerome', 'other': 'foo'})
        self.assertEqual(u.name, 'Jerome')
        self.assertIsInstance(u.name, str)
        self.assertIsInstance(u.name_encrypted, bytes)
        self.assertEqual(u.name_encrypted, TestMapped.encr('Jerome'))
        self.assertEqual(u.name, TestMapped.decr(u.name_encrypted))

        u2 = TestMapped.query.find({"name_encrypted": TestMapped.encr("Jerome")}).first()
        self.assertEqual(u._id, u2._id)

        u.name = 'Jessie'
        self.session.flush()
        self.assertEqual(u.name, 'Jessie')
        self.assertEqual(u.name_encrypted, TestMapped.encr('Jessie'))
        self.assertEqual(u.decrypt_some_fields(), {'_id': None, 'name': 'Jessie', 'other': 'foo'})
        self.assertIsNotNone(TestMapped.query.get(name_encrypted=TestMapped.encr('Jessie')))

        u.name_encrypted = TestMapped.encr('James')
        self.session.flush()
        self.assertEqual(u.name, 'James')
        self.assertEqual(u.name_encrypted, TestMapped.encr('James'))

        self.assertEqual(set(u.encrypted_field_names()), set(['name_encrypted']))
        self.assertEqual(set(u.decrypted_field_names()), set(['name']))
        self.assertEqual(set(u._field_names), set(['_id', 'name_encrypted', 'other']))


class TestMappingReal(TestMapping):
    DATASTORE = f"mongodb://localhost/test_ming_TestDocumentReal_{os.getpid()}?serverSelectionTimeoutMS=100"


class TestEncryptedFieldDocument(TestCase):
    """Tests for EncryptedField with Document models (non-ODM)."""
    DATASTORE = "mim://host/test_db"

    def setUp(self):
        import_formencode()

        ming.configure(**{
            'ming.test_db.uri': self.DATASTORE,
            'ming.test_db.encryption.kms_providers.local.key': make_encryption_key(self.__class__.__name__),
            'ming.test_db.encryption.key_vault_namespace': 'encryption_test.coll_key_vault_test',
            'ming.test_db.encryption.provider_options.local.key_alt_names': '["test_datakey_1"]'
        })

    def tearDown(self):
        session = ming.Session.by_name('test_db')
        session.bind.conn.drop_database('test_db')
        session.bind.conn.drop_database('encryption_test')

    def test_encrypted_dict_field(self):
        """Test EncryptedField with a dict schema containing encrypted fields."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_encrypted_dict'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'id': S.ObjectId,
                'username_encrypted': S.Binary,
                'display_name': str,
            })

        # Create doc and set encrypted field via descriptor
        doc = TestDoc.make(dict(_id=1))
        doc.author = {'id': None, 'username': 'john_doe', 'display_name': 'John Doe'}
        doc.m.save()

        # Verify decryption via wrapper
        self.assertEqual(doc.author.username, 'john_doe')
        self.assertEqual(doc.author.display_name, 'John Doe')

        # Verify storage is encrypted
        self.assertIsInstance(doc['author']['username_encrypted'], bytes)
        self.assertEqual(doc['author']['username_encrypted'], TestDoc.encr('john_doe'))

        # Update via wrapper
        doc.author.username = 'jane_doe'
        doc.m.save()
        self.assertEqual(doc.author.username, 'jane_doe')

    def test_encrypted_list_of_strings(self):
        """Test EncryptedField with a list of encrypted strings."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_encrypted_list_strings'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            secrets = EncryptedField('secrets', [S.Binary])

        doc = TestDoc.make(dict(_id=1))
        doc.secrets = ['secret1', 'secret2', 'secret3']
        doc.m.save()

        # Verify decryption
        self.assertEqual(list(doc.secrets), ['secret1', 'secret2', 'secret3'])

        # Verify storage is encrypted
        for item in doc['secrets']:
            self.assertIsInstance(item, bytes)

        # Test list operations
        doc.secrets.append('secret4')
        self.assertEqual(len(doc.secrets), 4)
        self.assertEqual(doc.secrets[3], 'secret4')

    def test_encrypted_list_of_dicts(self):
        """Test EncryptedField with a list of dicts containing encrypted fields."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_encrypted_list_dicts'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            contacts = EncryptedField('contacts', [{'name': str, 'phone_encrypted': S.Binary}])

        doc = TestDoc.make(dict(_id=1))
        doc.contacts = [
            {'name': 'Alice', 'phone': '123-456-7890'},
            {'name': 'Bob', 'phone': '098-765-4321'}
        ]
        doc.m.save()

        # Verify decryption
        self.assertEqual(doc.contacts[0].name, 'Alice')
        self.assertEqual(doc.contacts[0].phone, '123-456-7890')
        self.assertEqual(doc.contacts[1].name, 'Bob')
        self.assertEqual(doc.contacts[1].phone, '098-765-4321')

        # Verify storage is encrypted
        self.assertIsInstance(doc['contacts'][0]['phone_encrypted'], bytes)

    def test_nested_encrypted_dict(self):
        """Test EncryptedField with nested dicts containing encrypted fields."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_nested_encrypted'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'username_encrypted': S.Binary,
                'profile': {
                    'ssn_encrypted': S.Binary,
                    'address': str,
                },
                'avatar': S.Binary,  # Binary but no _encrypted suffix, should not be encrypted
            })

        doc = TestDoc.make(dict(_id=1))
        doc.author = {
            'username': 'john',
            'profile': {
                'ssn': '123-45-6789',
                'address': '123 Main St'
            },
            'avatar': b'avatar'
        }
        doc.m.save()

        # Verify decryption
        self.assertEqual(doc.author.username, 'john')
        self.assertEqual(doc.author.profile.ssn, '123-45-6789')
        self.assertEqual(doc.author.profile.address, '123 Main St')
        self.assertEqual(doc.author.avatar, b'avatar')

        # Verify storage is encrypted
        self.assertIsInstance(doc['author']['username_encrypted'], bytes)
        self.assertIsInstance(doc['author']['profile']['ssn_encrypted'], bytes)

    def test_non_encrypted_binary_field(self):
        """Test that binary fields without _encrypted suffix are not encrypted."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_non_encrypted_binary'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'username_encrypted': S.Binary,
                'avatar': S.Binary,  # Binary but no _encrypted suffix
            })

        avatar_bytes = b'raw binary avatar data'
        doc = TestDoc.make(dict(_id=1))
        doc.author = {
            'username': 'john',
            'avatar': avatar_bytes,
        }
        doc.m.save()

        # avatar should NOT be encrypted (stored as-is)
        self.assertEqual(doc['author']['avatar'], avatar_bytes)
        # username should be encrypted
        self.assertIsInstance(doc['author']['username_encrypted'], bytes)
        self.assertNotEqual(doc['author']['username_encrypted'], b'john')

    def test_encrypted_field_with_unset_value(self):
        """Test EncryptedField with unset value - gets schema default."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_unset_value'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'username_encrypted': S.Binary,
            })

        # Create doc without setting author field
        doc = TestDoc.make(dict(_id=1))
        doc.m.save()
        # author gets the default schema value (dict with None values)
        self.assertIn('author', doc)
        self.assertEqual(doc['author'], {'username_encrypted': None})
        # Wrapper returns None when accessing unset encrypted field
        self.assertIsNone(doc.author.username)

    def test_encrypted_empty_string(self):
        """Test that empty strings are properly encrypted."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_empty_string'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'username_encrypted': S.Binary,
            })

        doc = TestDoc.make(dict(_id=1))
        doc.author = {'username': ''}
        doc.m.save()

        # Empty string should be encrypted
        self.assertIsInstance(doc['author']['username_encrypted'], bytes)
        self.assertEqual(doc.author.username, '')

    def test_virtual_name_iteration(self):
        """Test that iterating over EncryptedDictWrapper uses virtual names."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_iteration'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'username_encrypted': S.Binary,
                'display_name': str,
            })

        doc = TestDoc.make(dict(_id=1))
        doc.author = {'username': 'john', 'display_name': 'John Doe'}
        doc.m.save()

        # keys() should return virtual names
        keys = list(doc.author.keys())
        self.assertIn('username', keys)
        self.assertIn('display_name', keys)
        self.assertNotIn('username_encrypted', keys)

        # items() should use virtual names
        items = dict(doc.author.items())
        self.assertEqual(items['username'], 'john')
        self.assertEqual(items['display_name'], 'John Doe')

    def test_encrypted_list_contains(self):
        """Test __contains__ on encrypted lists."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_list_contains'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            secrets = EncryptedField('secrets', [S.Binary])

        doc = TestDoc.make(dict(_id=1))
        doc.secrets = ['secret1', 'secret2']
        doc.m.save()

        self.assertIn('secret1', doc.secrets)
        self.assertIn('secret2', doc.secrets)
        self.assertNotIn('secret3', doc.secrets)

    def test_make_encr_with_encrypted_dict_field(self):
        """Test make_encr() with EncryptedField dict schema."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_make_encr_dict'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'id': int,
                'username_encrypted': S.Binary,
                'email_encrypted': S.Binary,
                'display_name': str,
            })

        # Use make_encr with virtual names - should encrypt properly
        doc = TestDoc.make_encr(dict(
            _id=1,
            author={
                'id': 42,
                'username': 'john_doe',
                'email': 'john@example.com',
                'display_name': 'John Doe',
            }
        ))
        doc.m.save()

        # Verify data is encrypted in storage
        self.assertIsInstance(doc['author']['username_encrypted'], bytes)
        self.assertIsInstance(doc['author']['email_encrypted'], bytes)
        self.assertEqual(doc['author']['username_encrypted'], TestDoc.encr('john_doe'))
        self.assertEqual(doc['author']['email_encrypted'], TestDoc.encr('john@example.com'))

        # Verify non-encrypted fields are preserved
        self.assertEqual(doc['author']['id'], 42)
        self.assertEqual(doc['author']['display_name'], 'John Doe')

        # Verify decryption via wrapper
        self.assertEqual(doc.author.username, 'john_doe')
        self.assertEqual(doc.author.email, 'john@example.com')
        self.assertEqual(doc.author.display_name, 'John Doe')

    def test_make_encr_with_encrypted_list_field(self):
        """Test make_encr() with EncryptedField list schema."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_make_encr_list'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            secrets = EncryptedField('secrets', [S.Binary])

        # Use make_encr with plaintext list items
        doc = TestDoc.make_encr(dict(
            _id=1,
            secrets=['secret1', 'secret2', 'secret3']
        ))
        doc.m.save()

        # Verify data is encrypted in storage
        for i, item in enumerate(doc['secrets']):
            self.assertIsInstance(item, bytes)
        self.assertEqual(doc['secrets'][0], TestDoc.encr('secret1'))

        # Verify decryption via wrapper
        self.assertEqual(list(doc.secrets), ['secret1', 'secret2', 'secret3'])

    def test_make_encr_with_list_of_dicts(self):
        """Test make_encr() with EncryptedField list of dicts schema."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_make_encr_list_dicts'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            contacts = EncryptedField('contacts', [{'name': str, 'phone_encrypted': S.Binary}])

        # Use make_encr with virtual names in list items
        doc = TestDoc.make_encr(dict(
            _id=1,
            contacts=[
                {'name': 'Alice', 'phone': '555-1234'},
                {'name': 'Bob', 'phone': '555-5678'},
            ]
        ))
        doc.m.save()

        # Verify data is encrypted in storage
        self.assertEqual(doc['contacts'][0]['name'], 'Alice')
        self.assertIsInstance(doc['contacts'][0]['phone_encrypted'], bytes)
        self.assertEqual(doc['contacts'][0]['phone_encrypted'], TestDoc.encr('555-1234'))

        # Verify decryption via wrapper
        self.assertEqual(doc.contacts[0].name, 'Alice')
        self.assertEqual(doc.contacts[0].phone, '555-1234')
        self.assertEqual(doc.contacts[1].name, 'Bob')
        self.assertEqual(doc.contacts[1].phone, '555-5678')

    def test_make_encr_mixed_fields(self):
        """Test make_encr() with both top-level and nested encrypted fields."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_make_encr_mixed'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            # Top-level encrypted field
            ssn_encrypted = Field(S.Binary)
            # Nested encrypted field
            profile = EncryptedField('profile', {
                'name': str,
                'email_encrypted': S.Binary,
            })

        # Use make_encr with virtual names
        doc = TestDoc.make_encr(dict(
            _id=1,
            ssn='123-45-6789',  # Top-level virtual name
            profile={
                'name': 'John',
                'email': 'john@example.com',  # Nested virtual name
            }
        ))
        doc.m.save()

        # Verify top-level is encrypted
        self.assertIsInstance(doc['ssn_encrypted'], bytes)
        self.assertEqual(doc['ssn_encrypted'], TestDoc.encr('123-45-6789'))
        self.assertNotIn('ssn', doc)

        # Verify nested is encrypted
        self.assertIsInstance(doc['profile']['email_encrypted'], bytes)
        self.assertEqual(doc['profile']['email_encrypted'], TestDoc.encr('john@example.com'))
        self.assertEqual(doc['profile']['name'], 'John')

        # Verify decryption for top-level (requires explicit decryption)
        self.assertEqual(TestDoc.decr(doc.ssn_encrypted), '123-45-6789')
        # Verify decryption for nested (automatic via wrapper)
        self.assertEqual(doc.profile.email, 'john@example.com')

    def test_make_encr_with_missing_encrypted_field(self):
        """Test make_encr() when EncryptedField is not provided in data."""
        class TestDoc(Document):
            class __mongometa__:
                name = 'test_make_encr_missing'
                session = ming.Session.by_name('test_db')
            _id = Field(S.Anything)
            author = EncryptedField('author', {
                'username_encrypted': S.Binary,
            })

        # Use make_encr without the encrypted field - should work
        doc = TestDoc.make_encr(dict(_id=1))
        doc.m.save()

        # The field gets default schema value
        self.assertEqual(doc['author'], {'username_encrypted': None})
        self.assertIsNone(doc.author.username)
