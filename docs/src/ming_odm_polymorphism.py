#!/usr/bin/env python
# Clear the class names in case MappedClasses are declared in another example
from ming.odm import Mapper
Mapper._mapper_by_classname.clear()

from ming import create_datastore
from ming.odm import ThreadLocalODMSession
session = ThreadLocalODMSession(bind=create_datastore('mim:///odm_tutorial'))

from ming import schema
from ming.odm import MappedClass
from ming.odm import FieldProperty, ForeignIdProperty, RelationProperty


class Transport(MappedClass):
    class __mongometa__:
        session = session
        name = 'transport'
        polymorphic_on = '_type'
        polymorphic_identity = 'base'

    _id = FieldProperty(schema.ObjectId)
    origin = FieldProperty(schema.String(required=True))
    destination = FieldProperty(schema.String(if_missing=''))
    _type = FieldProperty(schema.String(if_missing='base'))

    def move(self):
        return f'moving from {self.origin} to {self.destination}'


class Bus(Transport):
    class __mongometa__:
        polymorphic_identity = 'bus'

    _type=FieldProperty(str, if_missing='bus')
    passengers_count = FieldProperty(schema.Int(if_missing=0))

    def move(self):
        return f'driving from {self.origin} to {self.destination}'

class AirBus(Bus):
    class __mongometa__:
        polymorphic_identity = 'airbus'

    _type=FieldProperty(str, if_missing='airbus')
    wings_count = FieldProperty(schema.Int(if_missing=2))

    def move(self):
        return f'flying from {self.origin} to {self.destination}'

Transport.query.remove({})

#{compileall
from ming.odm import Mapper
Mapper.compile_all()
#}


def snippet1_1():
    # Create 2 Bus
    Bus(origin='Rome', destination='London', passengers_count=20)
    Bus(origin='Turin', destination='London', passengers_count=20)
    # And an AirBus
    AirBus(origin='Turin', destination='London', passengers_count=60, wings_count=3)
    session.flush()

def snippet1_2():
    session.clear()
    Transport.query.find().count()
    Transport.query.find({'_type': 'bus'}).count()

def snippet1_3():
    session.clear()
    Transport.query.find().all()

def snippet1_4():
    session.clear()
    transports = Transport.query.find().all()
    transports[0].move()
    transports[1].move()
    transports[2].move()
