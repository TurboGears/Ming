# Clear the class names in case MappedClasses are declared in another example
from ming.odm import Mapper
Mapper._mapper_by_classname.clear()

#{initial-imports
import re
from ming import create_datastore
from ming.odm import ThreadLocalODMSession

session = ThreadLocalODMSession(bind=create_datastore('mim:///odm_tutorial'))
#}

#{odm-imports
from ming import schema
from ming.odm import MappedClass
from ming.odm import FieldProperty, ForeignIdProperty
#}


class WikiPage(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_page'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String(required=True))
    text = FieldProperty(schema.String(if_missing=''))

class WikiComment(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_comment'

    _id = FieldProperty(schema.ObjectId)
    page_id = ForeignIdProperty('WikiPage')
    text = FieldProperty(schema.String(if_missing=''))

class WikiPageWithMetadata(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_page_with_metadata'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String(required=True))
    text = FieldProperty(schema.String(if_missing=''))

    metadata = FieldProperty(schema.Object({
        'tags': schema.Array(schema.String),
        'categories': schema.Array(schema.String)
    }))

WikiPage.query.remove({})
WikiComment.query.remove({})
WikiPageWithMetadata.query.remove({})

#{compileall
from ming.odm import Mapper
Mapper.compile_all()
#}

def snippet1():
    wp = WikiPage(title='FirstPage',
                  text='This is a page')
    wp

def snippet1_0():
    session

def snippet1_1():
    session.flush()
    session

def snippet1_2():
    session.clear()
    session


def snippet2():
    wp = WikiPage.query.get(title='FirstPage')

    # Note IdentityMap keeps only one copy of the object when they are the same
    wp2 = WikiPage.query.find({'text': 'This is a page'}).first()
    wp is wp2

def snippet2_1():
    # Create a new page to see find in action
    wp2 = WikiPage(title='SecondPage', text='This is a page')
    session.flush()

    WikiPage.query.find({'text': 'This is a page'}).count()
    WikiPage.query.find({'text': 'This is a page'}).first()
    WikiPage.query.find({'text': 'This is a page'}).all()

def snippet2_2():
    session.clear()
    WikiPage.query.find({'text': re.compile(r'^This')}, projection=('title',)).all()

def snippet2_3():
    WikiPage.query.find({}).first()
    WikiPage.query.find({}, refresh=True).first()

def snippet2_4():
    docs = WikiPage.query.find({'text': re.compile(r'^This')}, projection=('title',)).all()
    docs[0].title, docs[0].text
    docs[1].title, docs[1].text


def snippet5_1():
    session.clear()
    wp = WikiPage.query.get(title='FirstPage')
    session

    wp.title = 'MyFirstPage'
    # Notice that the object has been marked dirty
    session

    # Flush the session to actually apply the changes
    session.flush()

def snippet5_3():
    WikiPage.query.find_one_and_update({'title': 'MyFirstPage'},
                                       update={'$set': {'text': 'This is my first page'}},
                                       upsert=True)

def snippet5_4():
    wp = WikiPage.query.get(title='MyFirstPage')
    WikiPage.query.update({'_id': wp._id}, {'$set': {'text': 'This is my first page!!!'}})

    # Update doesn't fetch back the document, so
    # we still have the old value in the IdentityMap
    WikiPage.query.get(wp._id).text

    # Unless we refresh it.
    wp = session.refresh(wp)
    wp.text

def snippet5_5():
    wp = WikiPage.query.get(title='MyFirstPage')
    wp.delete()

    # We flush the session to actually delete the object
    session.flush()

    # The object has been deleted and so is not on the DB anymore.
    WikiPage.query.find({'title': 'MyFirstPage'}).count()

def snippet5_6():
    WikiPage.query.find().count()
    WikiPage.query.remove({})

    WikiPage.query.find().count()

def snippet4():
    wp = WikiPage.query.get(title='MyFirstPage')
    results = WikiComment.query.find(dict(page_id=wp._id))
    list(results)


def snippet6():
    from ming.odm import mapper
    wikipage_mapper = mapper(WikiPage)

    # Mapper.collection is the foundation layer collection
    founding_WikiPage = wikipage_mapper.collection

    # Retrieve the foundation layer session
    founding_Session = session.impl

    # The foundation layer still returns dictionaries, but validation is performed.
    founding_Session.find(founding_WikiPage, {'title': 'MyFirstPage'}).all()


def snippet7():
    from ming.odm import mapper
    mongocol = mapper(WikiPage).collection.m.collection
    mongocol

    mongocol.find_one({'title': 'MyFirstPage'})


def snippet8():
    wpm = WikiPageWithMetadata(title='MyPage')
    session.flush()

    # Get back the object to see that Ming creates the correct structure for us
    session.clear()
    wpm = WikiPageWithMetadata.query.get(title='MyPage')
    wpm.metadata

    # We can append or edit subdocuments like any other property
    wpm.metadata['tags'].append('foo')
    wpm.metadata['tags'].append('bar')
    session.flush()

    # Check that ming updated everything on flush
    session.clear()
    wpm = WikiPageWithMetadata.query.get(title='MyPage')
    wpm.metadata


def connection_session():
    from ming import create_datastore
    from ming.odm import ThreadLocalODMSession

    session = ThreadLocalODMSession(
        bind=create_datastore('mim:///tutorial')
    )
    session
    # The database and datastore are still available
    # through the session as .db and .bind
    session.db
    session.bind


def connection_configure():
    from ming import configure
    from ming.odm import ThreadLocalODMSession

    configure(**{'ming.mysession.uri': 'mongodb://localhost:27017/tutorial'})

    session = ThreadLocalODMSession.by_name('mysession')
    session.db

    ThreadLocalODMSession.by_name('mysession') is session
