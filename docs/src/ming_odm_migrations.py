# Clear the class names in case MappedClasses are declared in another example
from ming.odm import Mapper, mapper
Mapper._mapper_by_classname.clear()

#{initial-imports
from ming import create_datastore
from ming.odm import ThreadLocalODMSession

session = ThreadLocalODMSession(bind=create_datastore('mim:///odm_tutorial'))
#}

#{odm-imports
from ming import schema
from ming.odm import MappedClass
from ming.odm import FieldProperty, ForeignIdProperty
#}

#{migrate-newmodel
class WikiPage(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_page'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String(required=True))
    text = FieldProperty(schema.String(if_missing=''))

    metadata = FieldProperty(schema.Object({
        'tags': schema.Array(schema.String),
        'categories': schema.Array(schema.String)
    }))
#}

WikiPageWithoutMigration = WikiPage

#{migrate-oldschema
from ming import collection, Field

OldWikiPageCollection = collection('wiki_page', session,
    Field('_id', schema.ObjectId),
    Field('title', schema.String),
    Field('text', schema.String),
    Field('tags', schema.Array(schema.String))
)
#}

WikiPage.query.remove({})

#{migrate-model-with-migration
class WikiPage(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_page'
        version_of = OldWikiPageCollection

        @staticmethod
        def migrate(data):
            result = dict(data, metadata={'tags': data['tags']}, _version=1)
            del result['tags']
            return result

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String(required=True))
    text = FieldProperty(schema.String(if_missing=''))

    _version = FieldProperty(1, required=True)

    metadata = FieldProperty(schema.Object({
        'tags': schema.Array(schema.String),
        'categories': schema.Array(schema.String)
    }))
#}

class MyModel(MappedClass):
    class __mongometa__:
        session = session
        name = 'mymodel'
        version_of = collection('mymodel', session,
            Field('_id', schema.ObjectId),
            Field('name', schema.String),
            Field('_version', schema.Value(1, required=True)),
            version_of=collection('mymodel', session,
                Field('_id', schema.ObjectId),
                Field('name', schema.String),
            ),
            migrate=lambda data: dict(_id=data['_id'], name=data['name'].upper(), _version=1)
        )

        @staticmethod
        def migrate(data):
            return dict(_id=data['_id'], name=data['name'][::-1], _version=2)

    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(schema.String(required=True))

    _version = FieldProperty(2, required=True)

MyModel.query.remove({})


#{compileall
from ming.odm import Mapper
Mapper.compile_all()
#}


def snippet1():
    import random
    TAGS = ['foo', 'bar', 'snafu', 'mongodb']

    # Insert the documents through PyMongo so that Ming is not involved
    session.db.wiki_page.insert_many([
        dict(title='Page %s' % idx, text='Text of Page %s' %idx, tags=random.sample(TAGS, 2)) for idx in range(10)
    ])

    session.db.wiki_page.find_one()

def snippet2():
    WikiPage = WikiPageWithoutMigration
    WikiPage.query.find().first()

def snippet3():
    WikiPage.query.find().limit(3).all()

def snippet4():
    next(session.db.wiki_page.find())

def snippet5():
    next(session.db.wiki_page.find()).get('tags')

    from ming.odm import mapper
    mapper(WikiPage).collection.m.migrate()

    next(session.db.wiki_page.find()).get('metadata')

def snippet6():
    session.db.mymodel.insert_one(dict(name='desrever'))
    session.db.mymodel.find_one()

    # Apply migration to version 1 and then to version 2
    mapper(MyModel).collection.m.migrate()

    session.db.mymodel.find_one()