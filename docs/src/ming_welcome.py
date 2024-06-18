# Clear the class names in case MappedClasses are declared in another example
from ming.odm import Mapper
Mapper._mapper_by_classname.clear()

#{connect-imports
from ming import create_datastore
from ming.odm import ThreadLocalODMSession

session = ThreadLocalODMSession(
    bind=create_datastore('mim:///odm_welcome')
)
#}

#{odm-model
from ming import schema
from ming.odm import FieldProperty
from ming.odm.declarative import MappedClass

class WikiPage(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_page'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String(required=True))
    text = FieldProperty(schema.String(if_missing=''))
#}

#{compileall
from ming.odm import Mapper

Mapper.compile_all()
#}

WikiPage.query.remove({})


def snippet1():
    session.db.wiki_page.insert_one({'title': 'FirstPage',
                                     'text': 'This is my first page'})
    session.db.wiki_page.find_one({'title': 'FirstPage'})


def snippet2():
    # Creating a Document is enough to register it into the UnitOfWork
    WikiPage(title='FirstPage',
             text='This is my first page')
    # Flush the unit of work to save changes on DB
    session.flush()

    wp = WikiPage.query.find({'title': 'FirstPage'}).first()
    wp