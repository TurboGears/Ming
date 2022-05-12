#!/usr/bin/env python
# Clear the class names in case MappedClasses are declared in another example
import re
from ming.odm import Mapper
Mapper._mapper_by_classname.clear()

from ming import create_datastore
from ming.odm import ThreadLocalODMSession
session = ThreadLocalODMSession(bind=create_datastore('mim:///odm_tutorial'))

from ming import schema
from ming.odm import MappedClass
from ming.odm import FieldProperty
import hashlib


class PasswordProperty(FieldProperty):
    def __init__(self):
        # Password is always a required string.
        super().__init__(schema.String(required=True))

    def __get__(self, instance, cls=None):
        if instance is None: return self

        class Password(str):
            def __new__(cls, content):
                self = str.__new__(cls, '******')
                self.raw_value = content
                return self

        # As we don't want to leak passwords we return an asterisked string
        # but the real value of the password will always be available as .raw_value
        # so we can check passwords when logging in.
        return Password(super().__get__(instance, cls))

    def __set__(self, instance, value):
        pwd = hashlib.md5(value).hexdigest()
        super().__set__(instance, pwd)


class User(MappedClass):
    class __mongometa__:
        session = session
        name = 'user'

    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(schema.String(required=True))
    password = PasswordProperty()


User.query.remove({})

#{compileall
from ming.odm import Mapper
Mapper.compile_all()
#}


def snippet1_1():
    user = User(name='User 1',
                password='12345678')
    session.flush()

    user = session.db.user.find_one()
    user['password']
    user['password'] == hashlib.md5('12345678').hexdigest()

def snippet1_2():
    session.clear()
    user = User.query.find().first()
    user.password
    user.password.raw_value