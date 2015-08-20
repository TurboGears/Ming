:tocdepth: 3

=================
Ensuring Indexing
=================

The ODM layer permits to ensure indexes over the collections by using the
`__mongometa__` attribute. You can enforce both unique indexing and non-unique
indexing.

Indexing a Field
================

Indexing a field is possible by just setting ``index=True`` parameter when
creating a :class:`.FieldProperty`:

.. code-block:: python

    class Permission(MappedClass):
        class __mongometa__:
            session = session
            name = 'permissions'

        _id = FieldProperty(s.ObjectId)
        permission_name = FieldProperty(s.String, index=True)
        description = FieldProperty(s.String)
        groups = FieldProperty(s.Array(str))

More flexible indexes definition is also available through the
``indexes`` property of ``__mongometa__``:

.. code-block:: python

    class Permission(MappedClass):
        class __mongometa__:
            session = session
            name = 'permissions'
            indexes = [('permission_name', )]

        _id = FieldProperty(s.ObjectId)
        permission_name = FieldProperty(s.String)
        description = FieldProperty(s.String)
        groups = FieldProperty(s.Array(str))

Indexes are represented by tuples which can have one or more entries (to represent
compound indexes)::

    indexes = [('permission_name', 'groups'),]

Also the tuples can contain a ``(field, direction)`` declaration
to indicate where the index is sorted::

    indexes = [
        (('permission_name', ming.ASCENDING), ('groups', ming.DESCENDING))
    ]

Multiple indexes can be declared for a collection::

    indexes = [
        (('permission_name', ming.ASCENDING), ('groups', ming.DESCENDING)),
        ('groups', )
    ]

Unique Indexes
==============

You can use ``unique_indexes`` to ensure that it won't be possible to duplicate
an object multiple times with the same name:

.. code-block:: python

    class Permission(MappedClass):
        class __mongometa__:
            session = session
            name = 'permissions'
            unique_indexes = [('permission_name',)]

        _id = FieldProperty(s.ObjectId)
        permission_name = FieldProperty(s.String)
        description = FieldProperty(s.String)
        groups = FieldProperty(s.Array(str))

Custom Indexes
==============

If you want more control over your indexes, you can use custom_indexes directly within
`__mongometa__`, this will allow you to explicitly set unique and/or sparse index
flags that same way you could if you were directly calling ensureIndex in the MongoDB
shell. For example, if you had a field like email that you wanted to be unique, but
also allow for it to be Missing

.. code-block:: python

    class User(MappedClass):
        class __mongometa__:
            session = session
            name = 'users'
            custom_indexes = [
                dict(fields=('email',), unique=True, sparse=True)
            ]

        _id = FieldProperty(s.ObjectId)
        email = FieldProperty(s.String, if_missing=s.Missing)

Indexes definitions in ``custom_indexes`` can actually contain any
key which is a valid argument for :class:`.Index` initialization function as
they are used to actually create :class:`.Index` instances.

Now when accessing instances of User, if email is Missing and you attempt to use the
User.email attribute Ming, will throw an AttributeError as it ensures that only
properties that are not Missing are mapped as attributes to the class.

This brings us to the :class:`ming.odm.property.FieldPropertyWithMissingNone`
property type. This allows you to mimic the behavior that you commonly find in a SQL
solution. An indexed and unique field that is also allowed to be NULL
or in this case Missing. A classic example would be a product database where you
want to enter in products but don't have SKU numbers for them yet. Now your product listing
can still call product.sku without throwing an AttributeError.

.. code-block:: python

    class Product(MappedClass):
        class __mongometa__:
            session = session
            name = 'products'
            custom_indexes = [
                dict(fields=('sku',), unique=True, sparse=True)
            ]

        _id = FieldProperty(s.ObjectId)
        sku = FieldPropertyWithMissingNone(str, if_missing=s.Missing)

To apply the specified indexes you can then iterate over all the mappers and
call :meth:`.Mapper.ensure_indexes` over the mapped collection.

.. code-block:: python

    for mapper in ming.odm.Mapper.all_mappers():
        mainsession.ensure_indexes(mapper.collection)

This needs to be performed each time you change the indexes or the database.
It is common practice to ensure all the indexes at application startup.