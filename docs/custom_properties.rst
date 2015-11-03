:tocdepth: 3

=============================
Custom Schemas and Properties
=============================

Ming provides builtin properties for most common use cases: :class:`.RelationProperty`,
:class:`.ForeignIdProperty`, :class:`.FieldProperty`, :class:`.FieldPropertyWithMissingNone`
those allow you to define relations, references to other models, store common values
and get back properties that might not be available on the collection.

Also when validating data the :mod:`ming.schema` module provides schemas to validate
most common use cases and data types.

While those are usually enough, there might be cases where you want to define your own
properties or schemas to force better contraints or add conversions.

Custom Schema
=============

Schemas are only used to *enforce constraints*, they usually do not convert values and
are applied both when data is loaded from or saved to the database.

Validating with Schemas
-----------------------

Schemas can be easily implemented by subclassing :class:`.FancySchemaItem` which
already provides ``if_missing`` and ``required`` support. Then the actual validation
will be performed by a custom ``._validate`` method:

.. literalinclude:: src/ming_odm_schemas.py
    :pyobject: EmailSchema

Then objects can use that schema like any other:

.. literalinclude:: src/ming_odm_schemas.py
    :pyobject: Contact

And can be created and queried as usual:

.. run-pysnippet:: ming_odm_schemas snippet1_1

Trying to create a Contact with an invalid email address will fail as
it won't pass our schema validation:

.. run-pysnippet:: ming_odm_schemas snippet1_2

Schema validation is not only enforced on newly created items or when
setting properties, but also on data loaded from the database. This is
because as MongoDB is schema-less there is no guarantee that the data we
are loading back is properly formatted:

.. run-pysnippet:: ming_odm_schemas snippet1_3
    :skip: 1

Schemas can't convert
---------------------

As schemas are validated both when saving and loading data a good use case for
a custom schema might be checking that a field is storing a properly formatted
email address but it cannot be used for an hashed password.

Let's see what happens when we try to perform some kind of conversion inside
a schema. For example we might try to define a ``PasswordSchema``:

.. literalinclude:: src/ming_odm_schemas.py
    :pyobject: PasswordSchema

which is used by our ``UserWithSchema`` class:

.. literalinclude:: src/ming_odm_schemas.py
    :pyobject: UserWithSchema

Then we can create a new user and query it back:

.. run-pysnippet:: ming_odm_schemas snippet2_1

At first sight it might seem that everything worked as expected.
Our user got created and the password is actually an md5.

But is it the right md5?

.. run-pysnippet:: ming_odm_schemas snippet2_2
    :skip: 1

It looks like it isn't.
Actually when we query the user back we get something which is
even different from the value stored on the database:

.. run-pysnippet:: ming_odm_schemas snippet2_3
    :skip: 1

And what we have on the db is not even the md5 of our password:

.. run-pysnippet:: ming_odm_schemas snippet2_4

That's because our value is actually the md5 recursively applied
multiple times whenever the validation was performed:

.. run-pysnippet:: ming_odm_schemas snippet2_5
    :skip: 1

So what we learnt is that schemas should never be used to convert
values as they can be applied recursively any number of times whenever
the document is saved or loaded back!

So what can we use to convert values? **Custom Properties**

Custom Properties
=================

Custom Properties are specific to the ODM layer and are not available
on the :ref:`ming_baselevel` which implements only schema validation.

The benefit of custom properties over schemas is that you actually
know whenever the valid is read or saved and so they can be properly
used for conversion of values to and from python.

Converting with Properties
--------------------------

Ming Properties actually implement the Python **Descriptor Protocol**
which is based on ``__get__``, ``__set__``?and ``__delete__`` methods
to retrieve, save and remove values from an object.

So implementing a custom property is a matter of subclassing :class:`.FieldProperty`
and providing our custom behaviour:

.. literalinclude:: src/ming_odm_properties.py
    :pyobject: PasswordProperty

Then we can use it like any other property in our model:

.. literalinclude:: src/ming_odm_properties.py
    :pyobject: User

This is already enough to be able to store properly hashed passwords:

.. run-pysnippet:: ming_odm_properties snippet1_1

And as we provided some kind of password leakage prevention by always
returning an asterisked string for the password let's check if it works
as expected:

.. run-pysnippet:: ming_odm_properties snippet1_2
    :skip: 1

As we can see the password is properly returned as a ``Password`` instance
which is a string with asterisk that also provides the real value as ``.raw_value``.