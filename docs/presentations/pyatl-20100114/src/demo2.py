from ming.datastore import DataStore
from ming import Session

bind = DataStore('mongodb://localhost:27017/test_database')
session = Session(bind)

from ming import Field, Document, schema
import datetime

class WikiPage(Document):
    class __mongometa__:
        session = session
        name = 'pages'
    _id = Field(schema.ObjectId)
    author = Field(str)
    title = Field(str)
    tags = Field([str])
    date = Field(datetime.datetime)
    text = Field(str)

page = WikiPage.m.find().one()

page

page.author = 'Rick'

page.m.save()

page = WikiPage.m.find().one()

page

page2 = WikiPage.make(dict(
        title='SecondPage',
        author='Rick Copeland',
        tags=['ming', 'mongodb'],
        date=datetime.datetime.utcnow(),
        text='This should be a page about Ming'))

page2.m.save()

page2.foo = 5

page2.m.save()
