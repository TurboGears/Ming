:tocdepth: 3

.. _odm-polymorphic:

====================
Polymorphic Entities
====================

Introduction
============

Polymorphic entities are entities that inherit from the same base
model, are stored in the same collection but can have different
properties and behaviours depending on a guard value.

They are usually available to provide inheritance and specialised
behaviours in models as you would for Python subclasses.

Declaring Polymorphic Entities
==============================

First we need to declare our base model which will provide the
common properties and the property on which the model identity
is recognized:

.. literalinclude:: src/ming_odm_polymorphism.py
    :pyobject: Transport
    :emphasize-lines: 5, 6, 11

The ``_type`` property is used to store the identity
of the model, we usually won't be writing this property,
it will be automatically filled using ``if_missing`` with
a different value for each subclass.

``__mongometa__.polymorphic_on`` attribute is used to tell
Ming on which property the polymorphism is happening and
``__mongometa__.polymorphic_identity`` will be provided
by each class to tell which value of ``_type`` is bound
to that class:

.. literalinclude:: src/ming_odm_polymorphism.py
    :pyobject: Bus
    :emphasize-lines: 3, 5

.. literalinclude:: src/ming_odm_polymorphism.py
    :pyobject: AirBus
    :emphasize-lines: 3, 5

Now we can create ``Bus`` or ``AirBus`` instances freely
and use the additional properties provided by each one of them:

.. run-pysnippet:: ming_odm_polymorphism snippet1_1

When querying them back we can see that they all ended up
in the ``Transport`` collection and that as expected only
two ``Bus`` instances got created (the third is an ``AirBus``):

.. run-pysnippet:: ming_odm_polymorphism snippet1_2

Querying Polymorphic Entities
=============================

When querying back polymorphic entities you should always
query them back from their base type (in this case ``Transport``)
as the type promotion will be automatically performed by ming:

.. run-pysnippet:: ming_odm_polymorphism snippet1_3
    :skip: 1

As types are properly promoted it is also possible to rely
on behaviours specific of each single subclass:

.. run-pysnippet:: ming_odm_polymorphism snippet1_4
    :skip: 1