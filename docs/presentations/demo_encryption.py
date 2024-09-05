import bson
from ming import Session
from ming.datastore import create_engine, create_datastore, DataStore
from ming.encryption import EncryptionConfig
import ming.schema as S
from ming.tests import make_encryption_key

bind: DataStore = create_datastore(
    'mongodb://localhost:27017/test_database',
    encryption=EncryptionConfig({
            'kms_providers': {
                'local': {
                    # Don't use this for production! This is just for demo purposes
                    'key': make_encryption_key('demo_encryption'),
                },
            },
            'key_vault_namespace': 'demo_encryption_db.__keyVault',
            'provider_options': {
                'local': {
                    'key_alt_names': ['datakeyName'],
                },
            },
        }))

# clean up for our demo purposes
bind.conn.drop_database('test_database')
bind.conn.drop_database('demo_encryption_db')

session = Session(bind)

from ming import Field, Document, schema
from ming.encryption import DecryptedField
import datetime

class UserEmail(Document):
    class __mongometa__:
        session = session
        name = 'user_emails'
    _id = Field(schema.ObjectId)

    # Encrypted fields should:
    #  - Have '_encrypted' suffix
    #  - Have type Binary
    email_encrypted = Field(S.Binary, if_missing=None)

    # Decrypted fields should:
    #  - Have no suffix
    #  - Have the actual type
    #  - Provide the encrypted field's full name
    email = DecryptedField(str, 'email_encrypted')


user_email = UserEmail.make({})
assert not user_email.email
assert not user_email.email_encrypted

# Can directly set DecryptedField and it will auto-populate and encrypt its counterpart
user_email.email = 'rick@example.com'
assert user_email.email_encrypted is not None
assert user_email.email_encrypted != 'rick@example.com'
assert isinstance(user_email.email_encrypted, bson.Binary)
user_email.m.save()


# Use .make_encr to properly create new instance with unencrypted data
user_email2 = UserEmail.make_encr(dict(
        email='stacy@example.com'))


assert user_email2.email_encrypted is not None
assert user_email2.email_encrypted != 'stacy@example.com'
assert isinstance(user_email2.email_encrypted, bson.Binary)
blob1 = user_email2.email_encrypted

user_email2.m.save()

# updating the email updates the corresponding encrypted field
user_email2.email = 'stacy+1@example.com'
assert user_email2.email_encrypted != blob1

user_email2.m.save()

bind.conn.drop_database('test_database')
bind.conn.drop_database('demo_encryption_db')