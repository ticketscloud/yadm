===========================
Yet Another Document Mapper
===========================

.. image:: https://travis-ci.org/zzzsochi/yadm.svg?branch=master
    :target: https://travis-ci.org/zzzsochi/yadm

.. image:: https://coveralls.io/repos/zzzsochi/yadm/badge.png
    :target: https://coveralls.io/r/zzzsochi/yadm


It's small and simple ODM for use with MongoDB.

Full documentation: http://yadm.readthedocs.org


-----------
Quick start
-----------

.. code:: python

    import pymongo
    from yadm import Database, Document, fields


    # Create model
    class BlogPost(Document):
        __collection__ = 'blog_posts'

        title = fields.StringField()
        body = fields.StringField()


    # Create post
    post = BlogPost()
    post.title = 'Small post'
    post.body = 'Bla-bla-bla...'

    # Connect to database
    client = pymongo.MongoClient('localhost', 27017)
    db = Database(client, 'test')

    # Insert post to database
    db.insert(post)

    # Query posts
    qs = db(BlogPost).find({'title': {'$regex': '^S'}})
    assert qs.count() > 0

    for post in qs:
        assert post.title.startswith('S')

    # Query one post
    post = db(BlogPost).find_one({'title': 'Small post'})

    # Change post
    post.title = 'Bla-bla-bla title'

    # Save changed post
    db.save(post)


CHANGES
=======

1.1.4 (2016-08-20)
------------------

* Add some features to ``Bulk``:

  - ``Bulk.update_one(document, **kw)``: method for add update one document in bulk;
  - ``Bulk.find(query).update(**kw)``: update many documents by query;
  - ``Bulk.find(query).upsert().update(**kw)``: upsert document;
  - ``Bulk.find(query).remove(**kw)``: remove documents;


1.1.3 (2016-07-23)
------------------

* Add ``QuerySet.ids`` method for get only documents id's from queryset;

* Add ``Money.total_cents`` method and ``Money.from_cents`` classmethod;


1.1 (2016-04-26)
----------------

* Add cacheing on queryset level and use it for ``ReferenceField``;

* Add mongo aggregation framework support;

* Add ``read_preference`` setting;

* Add ``exc`` argument to ``QuerySet.find_one`` for raise exception if not found;

* Add ``multi`` argument to ``QuerySet.remove``;

* Deprecate ``QuerySet.with_id``;

* Refactoring.


1.0 (2015-11-14)
----------------

* Change document structure. No more bad `BaseDocument.__data__` attribute:
    - `BaseDocument.__raw__`: raw data from mongo;
    - `BaseDocument.__cache__`: cached objects, casted with fields;
    - `BaseDocument.__changed__`: changed objects.

* Changes api for custom fields:
    - Not more need create field descriptors for every field;
    - `prepare_value` called only for setattr;
    - `to_mongo` called only for save objects to mongo;
    - `from_mongo` called only for load values from `BaseDocument.__raw__`;
    - Remove `Field.default` attribute. Use `Field.get_default` method;
    - Add `Field.get_if_not_loaded` and `Field.get_if_attribute_not_set` method;
    - By default raise `NotLoadedError` if field not loaded from projection;

* Changes in `ReferenceField`:
    - Raise `BrokenReference` if link is bloken;
    - Raise `NotBindingToDatabase` if document not saved to database;

* `smart_null` keyword for `Field`;

* Fields in document must be instances (not classes!);

* Remove `ArrayContainer` and `ArrayContainerField`;

* Remove old `MapIntKeysField` and `MapObjectIdKeysField`. Use new `MapCustomKeysField`;

* Add `Database.update_one` method for run simple update query with specified document;

* Add `QuerySet.distinct`;

* `serialize.from_mongo` now accept `not_loaded` sequence with filed names who must mark as not loaded, `parent` and `name`;

* `serialize.to_mongo` do not call `FieldDescriptor.__set__`;

* Fakers! Subsystem for generate test objects;

* Tests now use pytest;

* And more, and more...
