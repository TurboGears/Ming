from pymongo import Connection

connection = Connection()

connection.drop_database('test_database')

db = connection.test_database

collection = db.test_collection

import datetime

page = dict(
    author='Rick',
    title='My first wiki page',
    tags=['mongodb', 'python', 'pymongo'],
    date=datetime.datetime.utcnow(),
    text='This is a wiki page')

pages = db.pages

pages.insert(page)

db.collection_names()

page = pages.find_one()

page['author'] = 'Rick Copeland'

pages.save(page)

pages.find_one()
