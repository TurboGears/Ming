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

class EmailSchema(schema.FancySchemaItem):
    regex = re.compile(r'[\w\.\+\-]+\@[\w]+\.[a-z]{2,3}$')

    def _validate(self, value, **kw):
        if not self.regex.match(value):
            raise schema.Invalid('Not a valid email address', value)
        return value


class Contact(MappedClass):
    class __mongometa__:
        session = session
        name = 'contact'

    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(schema.String(required=True))
    email = FieldProperty(EmailSchema)


class PasswordSchema(schema.FancySchemaItem):
    def _validate(self, value, **kwargs):
        return hashlib.md5(value).hexdigest()


class UserWithSchema(MappedClass):
    class __mongometa__:
        session = session
        name = 'user_with_schema'

    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(schema.String(required=True))
    password = FieldProperty(PasswordSchema)


Contact.query.remove({})
UserWithSchema.query.remove({})

#{compileall
from ming.odm import Mapper
Mapper.compile_all()
#}


def snippet1_1():
    Contact(name='Contact 1', email='contact1@server.com')
    session.flush()
    session.clear()

    c = Contact.query.find().first()
    c.email

def snippet1_2():
    try:
        Contact(name='Contact 1', email='this-is-invalid')
    except schema.Invalid as e:
        error = e

    error

def snippet1_3():
    Contact.query.remove({})
    session.db.contact.insert_one(dict(name='Invalid Contact',
                                       email='this-is-invalid'))

    try:
        c1 = Contact.query.find().first()
    except schema.Invalid as e:
        error = e

    error


def snippet2_1():
    user = UserWithSchema(name='User 1',
                          password='12345678')
    session.flush()
    session.clear()

    user = UserWithSchema.query.find().first()
    user.password

def snippet2_2():
    session.clear()
    user = UserWithSchema.query.find().first()
    hashlib.md5('12345678').hexdigest() == user.password

def snippet2_3():
    session.clear()
    user = UserWithSchema.query.find().first()
    user.password

    user = session.db.user_with_schema.find_one()
    user['password']

def snippet2_4():
    user = session.db.user_with_schema.find_one()
    user['password']

    hashlib.md5('12345678').hexdigest()

def snippet2_5():
    session.clear()
    user = UserWithSchema.query.find().first()

    pwdmd5 = hashlib.md5('12345678').hexdigest()
    for i in range(10):
        if pwdmd5 == user.password:
            break
        pwdmd5 = hashlib.md5(pwdmd5).hexdigest()

    pwdmd5 == user.password
    i
