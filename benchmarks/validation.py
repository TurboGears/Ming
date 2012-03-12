import sys
import time
from datetime import datetime
from ming import Document, Field, Session
from ming import schema as S
from ming.datastore import DataStore

NUM_ITER = 10

doc_session = Session.by_name('benchmark')

doc_session.bind = DataStore('mongodb://localhost:27017', database='benchmark')

class StringNotNone(S.String):
    '''
    Stronger than if_missing='', this also converts an
    explicit None to ''
    '''
    def validate(self, value, **kw):
        if value == None or value is S.Missing:
            value = ''
        return S.String.validate(self, value, **kw)

class Project(Document):
    class __mongometa__:
        session = doc_session
        name='projects'
        indexes=[
            ('shortname',),
            ('source',),
            ('sf_id',),
        ]
        unique_indexes=[
            ('shortname', 'source'),
        ]

    _review = dict(
        rating=int, useful=float, useless=float, approved=bool,
        user=str, comments=str, safe_html=bool, source=str,
        usefulness=float, date=datetime)
    _screenshot = dict(url=str, thumb=str, name=str, description=str)
    _category = dict(id=int, shortname=str, name=str, description=str, fullpath=str)
    _resource = dict(url=str, name=str, feed=str,
                     item_count=int, item_open_count=int)
    _person = dict(
        username=None,
        homepage=None,
        name=None)
    _id=Field(S.ObjectId)
    shortname=Field(str)
    source=Field(str)
    sf_id=Field(int)
    projecttype=Field(int)
    private=Field(S.Bool(if_missing=False))
    name=Field(str)
    summary=Field(StringNotNone)
    created=Field(datetime)
    description=Field(StringNotNone)
    doap=Field(str)
    project_url=Field(str)
    homepage=Field(str)
    updated=Field(S.Deprecated)
    _last_changed=Field('last_changed', datetime)
    ad_keywords=Field([[str]]) # ['ohl', 'ad20848'] would translate to "ohl=ad20848;" in JS
    download_info=Field(S.Deprecated)
    _icon_url=Field('icon_url', S.String(if_missing=S.Missing)) # for backward compat.
    _features=Field('features', S.Array(str, if_missing=S.Missing)) # for backward compat.
    reviews_disabled=Field(bool)
    relations_data=Field(S.Object(dict(
        is_admin=S.Deprecated,
        rating=float,
        code=int,
        review_count=int,
        features=[str],
        tags=[dict(count=int, tag=str, approved=bool)],
        icon_url=str,
        latest_reviews=[_review],
        name=str,
        reviews=[_review],
        text=str), if_missing=None))
    related=Field([dict(source=None, shortname=None, name=str, description=str,
                        screenshots=[_screenshot], ok_to_recommend=bool,
                        rating=float, review_count=int, icon_url=str) ])
    recommended=Field([dict(source=None, shortname=None, name=str, description=str)])
    screenshots=Field([ _screenshot ])
    resources=Field(dict(
            other=[_resource],
            mailing_lists=[_resource],
            news=[_resource],
            forums=[_resource],
            trackers=[_resource],
            ))
    feed_recent_items=Field([
                dict(
                    _id=str,
                    description=str,
                    title=str,
                    url=str,
                    project=dict(source=str, shortname=str),
                    date=datetime,
                    type=str,
                    description_type=str,
                    author_username=str,
                    permission_required=S.Array(str, if_missing=None), # controllers/project.py queries mongodb for None which doens't match []
                )
            ])
    categories=Field({
            'Topic':[_category],
            'Operating System':[_category],
            'Development Status':[_category],
            'License':[_category],
            'Translations':[_category],
            'Intended Audience':[_category],
            'User Interface':[_category],
            'Programming Language':[_category],
            'Database Environment':[_category],
            })
    feeds_last_item=Field(S.Migrate(
            None, [ dict(url=str, date=datetime) ],
            S.Migrate.obj_to_list('url', 'date')))
    inactive=Field(datetime)
    new_project_url=Field(str)
    donation_page=Field(str)
    preferred_support=Field(str)
    code_repositories=Field([
            dict(
                label=str,
                browse=str,
                write_operations=int,
                read_operations=int,
                location=str,
                type=str) ])
    releases=Field([dict(
                filename=str,
                url=str,
                date=datetime,
                bytes=float,
                download_count=S.Deprecated,
                file_type=S.String(if_missing=''),
                mime_type=str,
                md5sum=str,
                sf_download_label=str, sf_platform_default=[str], sf_release_notes_file=str,
                sf_file_id=int,
                # FRS data (pre-PFS)
                sf_release_id=int, sf_package_id=int, sf_type=str, sf_platform=[str],
                release_notes_url=str,
                # old FRS data (shouldn't exist any more)
                group=S.Deprecated, #str,
                version=S.Deprecated, #str,
                changelog=S.Deprecated, #str,
                release_notes=S.Deprecated, #str,
            )])
    download_page=Field(str)
    screenshot_page=Field(str)
    maintainers=Field([_person])
    developers=Field([_person])
    file_feed=Field(str)
    awards=Field([ dict(category=str, url=str, event=str, img_url=str) ])
    sf_piwik_siteid=Field(str)
    license=Field(S.Deprecated())
    license_uri=Field(str)
    license_title=Field(str)
    developer_page=Field(str)

    test_foo2=Field(S.Deprecated)
    fossforus_id=Field(S.Deprecated)
    fossforus_screenshots=Field(S.Deprecated)
    fossforus_features=Field(S.Deprecated)
    fossforus_tags=Field(S.Deprecated)
    fossforus_ratings=Field(S.Deprecated)
    _last_snapshot_id=Field(S.Deprecated)

print 'Begin test'
sys.stdout.flush()
begin = time.time()
for x in xrange(NUM_ITER):
    Project.m.find(validate=True).next()
elapsed = time.time() - begin
docs_per_s = float(NUM_ITER) / elapsed
ms_per_doc = 1000 / docs_per_s
print 'Validated %d docs in %d secs (%.2f docs/s, %d ms/doc)' % (
    NUM_ITER, elapsed, docs_per_s, ms_per_doc)

