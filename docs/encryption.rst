:tocdepth: 3

.. _odm-encryption:

============================
Encrypting Sensitive Data
============================

This section describes how Ming can be used to automatically encrypt and decrypt your document's fields. This is accomplished by leveraging MongoDB's `Client-Side Field Level Encryption (CSFLE)`_ feature.



.. _Client-Side Field Level Encryption (CSFLE): https://pymongo.readthedocs.io/en/stable/examples/encryption.html#client-side-field-level-encryption


Encryption at the Foundation Level
==================================

When declaratively working with models by subclassing :class:`~ming.declarative.Document` in the :ref:`ming_baselevel`, you can add field level encryption by pairing a :class:`~ming.encryption.DecryptedField` with a :class:`~ming.metadata.Field`.


A simple example might look like the following.

.. code-block:: python

    class UserEmail(Document):
        class __mongometa__:
            session = session
            name = 'user_emails'
        _id = Field(schema.ObjectId)

        email_encrypted = Field(S.Binary, if_missing=None)
        email = DecryptedField(str, 'email_encrypted')


Breaking down DecryptedField
----------------------------------

This approach requires that you follow a few conventions:

#. The field storing the encrypted data should be configured in the following way:

   * It should be a :class:`~ming.metadata.Field`.
   * The Field should be of type :class:`~ming.schema.Binary`.
   * The Field's name should end with `_encrypted`.

#. Next to this should be a corresponding :class:`~ming.encryption.DecryptedField` that will decrypt the data.

   * Its first argument should be the type that you expect the decrypted data to be (`str`, `int`, etc.).
   * The second argument should be the name of the encrypted field (e.g. `email_encrypted`).
   * The DecryptedField's name should be the same as the encrypted :class:`~ming.metadata.Field`, but without the `_encrypted` suffix (e.g. `email`).


Encryption at the Declarative Level
========================================

Similarly when working with the higher level of abstraction offered by :class:`~ming.odm.declarative.MappedClass`es, you can add field level encryption by pairing a :class:`~ming.odm.declarative.DecryptedProperty` with a :class:`~ming.odm.property.FieldProperty`


A simple example might look like the following.

.. code-block:: python
    
        class UserEmail(MappedClass):
            class __mongometa__:
                session = session
                name = 'user_emails'
            _id = FieldProperty(schema.ObjectId)
    
            email_encrypted = FieldProperty(S.Binary, if_missing=None)
            email = DecryptedProperty(str, 'email_encrypted')


Breaking down DecryptedProperty
----------------------------------

Similarly to the foundation level, this approach requires that you follow a few conventions:

#. The field storing the encrypted data should be configured in the following way:

   * It should be a :class:`~ming.odm.property.FieldProperty`.
   * The FieldProperty should be of type :class:`~ming.schema.Binary`.
   * The FieldProperty's name should end with `_encrypted`.

#. Next to this should be a :class:`~ming.odm.declarative.DecryptedProperty` that will decrypt the data.

   * Its first argument should be the type that you expect the decrypted data to be (`str`, `int`, etc.).
   * The second argument should be the name of the encrypted field (e.g. `email_encrypted`).
   * The DecryptedProperty's name should be the same as the encrypted :class:`~ming.odm.declarative.DecryptedProperty`, but without the `_encrypted` suffix (e.g. `email`).
