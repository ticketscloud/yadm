===========================
Yet Another Document Mapper
===========================

.. image:: https://travis-ci.org/zzzsochi/yadm.svg?branch=master
    :target: https://travis-ci.org/zzzsochi/yadm

.. image:: https://coveralls.io/repos/github/zzzsochi/yadm/badge.svg?branch=master
    :target: https://coveralls.io/github/zzzsochi/yadm?branch=master



It's small and simple ODM for use with MongoDB.

.. Full documentation: http://yadm.readthedocs.org


------------
Requirements
------------

YADM support MongoDB version 3.x only. MongoDB 2.x is not supported.

Minimal version of python — 3.6.


-----------
Quick start
-----------

Create the models
=================

.. code:: python

    from datetime import datetime

    import pymongo

    from yadm import Database
    from yadm import Document, EmbeddedDocument
    from yadm import fields
    from yadm.serialize import to_mongo


    class User(Document):
        __collection__ = 'users'

        name = fields.StringField()
        email = fields.EmailField()


    class PostLogItem(EmbeddedDocument):
        op = fields.StringField(choices=['created', 'comment_added'])
        at = fields.DatetimeField()
        data = fields.MongoMapField()


    class Post(Document):
        __collection__ = 'posts'

        user = fields.ReferenceField(User)
        created_at = fields.DatetimeField(auto_now=True)
        title = fields.StringField()
        body = fields.StringField()
        log = fields.ListField(fields.EmbeddedDocumentField(PostLogItem))


    class Comment(Document):
        __collection__ = 'comments'

        user = fields.ReferenceField(User)
        created_at = fields.DatetimeField(auto_now=True)
        post = fields.ReferenceField(Post)
        text = fields.StringField()

All documents creates from class ``Document``. You can use multiple inheritance.

``__collection__`` magic attribute setups the collection name for documents of model.


Connect to database
===================

.. code:: python

    client = pymongo.MongoClient('mongodb://localhost:27017')
    db = Database(client, 'blog')

``Database`` object is a wrapper about ``pymongo`` or ``motor`` ``Database``.


Create documents
================

User
----

.. code:: python

    user = User(name='Bill', email='bill@galactic.hero')
    db.insert_one(user)

Just insert document to database.


Post
----

.. code:: python

    post = Post()
    post.user = user
    post.title = 'Small post'
    post.body = 'Bla-bla-bla...'
    post.log = [PostLogItem(op='created', at=datetime.utcnow())]
    db.insert_one(post)

You can fill documents as above.


Comment the post
----------------

.. code:: python

    comment = Comment()
    comment.user = user
    comment.post = post
    comment.text = "RE: Bla-bla-bla..."
    db.insert_one(comment)
    db.update_one(post, push={
        'log': to_mongo(PostLogItem(op='comment_added',
                                    at=comment.created_at,
                                    data={
                                      'comment': comment.id,
                                      'user': comment.user.id,
                                    }))
    })

We add log item to post's log. This is very usefull case.


Queries
=======

find
----

.. code:: python

    qs = db(Post).find({'title': {'$regex': '^S'}})
    assert qs.count() > 0

1. ``db(Post)`` creates the ``QuerySet`` object;
2. ``find`` method get the raw-query and return new ``QuerySet`` object with updated criteria;
3. ``count`` method make the query to database and return value.

.. code:: python

    for post in qs:
        assert post.title.startswith('S')

``__iter__`` method make the ``find``-query and returns the generator of documents.


find_one
--------

Get the first finded document.

.. code:: python

    post = db(Post).find_one({'user': user.id})


get_document
------------

Get the document by id from primary.

.. code:: python

    user = db.get_document(User, user.id)


References
----------

.. code:: python

    user = post.user

Get attribute with reference makes the query to referred collection. Warning: N+1 problem!
We have a cache in ``QuerySet`` object and get one referred document only once for one queryset.


Lookups
-------

.. code:: python

    comments = db(Comment).find({'post': post.id}).sort(('created_at', 1))
    for comment in comments.lookup('user'):
        print(comment.user.name, comment.text)

This code create the aggregate query with ``$lookup`` statement for resolve the references.


Aggregations
------------

.. code:: python

    agg = (db.aggregate(Comment)
           .match(user=user.id)
           .group(_id='post', count={'$sum': 1})
           .sort(count=-1))

    for item in agg:
        print(item)

Or traditional MongoDB syntax:

.. code:: python

    agg = db.aggregate(Comment, pipeline=[
        {'match': {'user': user.id}},
        {'group': {'_id': 'post', 'count': {'$sum': 1}}},
        {'sort': {'count': -1}},
    ])


-------
CHANGES
-------

2.0.4 (2019-02-20)
==================

* Add ``Database.estimated_document_count`` method for quickly count documents in the collection.


2.0.1 (2018-11-04)
==================

* Add ``QuerySet.hint`` for specify index for query.


2.0.0 (2018-10-25)
==================

* A `Big Rewrite <https://www.youtube.com/watch?v=xCGu5Z_vaps>`_ document logic:
    - ``Document.__raw__`` now contains only data from pymongo, without any ``AttributeNotSet`` or ``NotLoaded``;
    - ``Document.__changed__`` is removed: all changes reflects to ``Document.__cache__``;
    - ``Document.__not_loaded__`` frozenset of fields whitch not loaded by projection;
    - ``Document.__new_document__`` flag is ``True`` for document's objects whitch created directly in your code;
    - ``Document.__log__`` list-like container with log of document changes (unstable API at now);
    - ``Document.__data__`` is removed as deprecated;
    - Now is not allow to set fields as classes;
    - Defaults is not lazy and creates with document instance;

* Update for minimal versions of pymongo (3.7) and motor (2.0):
    - Add ``Database.bulk_write``;
    - Add ``Database.insert_one``, ``Database.insert_many`` and ``Database.delete_one``;
    - Deprecate ``Database.insert``, ``Database.remove``;
    - Remove ``Database.bulk`` (without deprecation period, sorry);
    - Add ``QuerySet.count_documents``;
    - Add ``QuerySet.update_one`` and ``QuerySet.update_many``;
    - Add ``QuerySet.delete_one`` and ``QuerySet.delete_many``;
    - Add ``QuerySet.find_one_and_update``, ``QuerySet.find_one_and_replace`` and ``QuerySet.find_one_and_delete``;
    - Deprecate ``QuerySet.count``;
    - Deprecate ``QuerySet.update``, ``QuerySet.remove`` and ``QuerySet.find_and_modify``;
    - Remove deprecated ``QuerySet.with_id``;

* Simple interface for build lookups: ``QuerySet.lookup``;
* Remove ``bcc`` argument from ``MoneyField``;
* Add ``Decimal128Field``.


1.5.0 (2017-12-31)
==================

* Experimental ``asyncio`` support;
* Add ``ReferencesListField`` for lists of references.


1.4.15 (2017-12-27)
===================

* Add ``projection`` argument to ``Database.get_document`` and ``Database.reload``;
* Add ``Document.__default_projection__`` attribute.


1.4.14 (2017-11-06)
===================

* Add ``EnumField`` for save ``enum.Enum``;
* Add ``EnumStateField`` for simple state machines based on ``enum.Enum``.


1.4.13 (2017-10-31)
===================

* Add ``QuerySet.batch_size`` method for setup batch size for cursor;
* Some minor fixes.



1.4.10 (2017-07-07)
==================

* ``ReferenceField.from_mongo`` try to get document from primary
    if not found by default.


1.4.9 (2017-07-06)
==================

* Add ``QuerySet.read_primary`` method for simple setup ``read_preference.Primary``.


1.4.4 (2017-05-17)
==================

* Add ``TimedeltaField`` for stores durations;
* Add ``SimpleEmbeddedDocumentField`` for simply create embedded documents.

.. code:: python

    class Doc(Document):
        embedded = SimpleEmbeddedDocumentField({
            'i': IntegerField(),
            's': StringField(),
        })


1.4.3 (2017-05-14)
==================

* Add ``StaticField`` for static data.


1.4.2 (2017-04-09)
==================

* Additional arguments (like ``write_concern``) for write operations;
* ``create_fake`` save the documents with write concern "majority" by default.


1.4.0 (2017-04-05)
==================

* Drop pymongo 2 support;
* Additional options for databases and collections;
* Add ``Database.get_document``;
* Add ``TypedEmbeddedDocumentField``;
* ``reload`` argument of ``Database.update_one`` must be keyword
    (may be backward incompotable).


1.3.1 (2017-02-21)
==================

* Change raw data for ``Money``;


1.3.0 (2017-02-19)
==================

* Add currency support to ``Money``:
    - Totaly rewrite ``Money`` type. Now it is not subclass of ``Decimal``;
    - Add storage for currencies: ``yadm.fields.money.currency.DEFAULT_CURRENCY_STORAGE``;


1.2.1 (2017-01-19)
==================

* Add ``QuerySet.find_in`` for ``$in`` queries with specified order;


1.2.0 (2016-12-27)
==================

* Drop MongoDB 2.X suport;
* Objects for update and remove results;
* Use Faker instead fake-factory.


1.1.4 (2016-08-20)
==================

* Add some features to ``Bulk``:
    - ``Bulk.update_one(document, **kw)``: method for add update one document in bulk;
    - ``Bulk.find(query).update(**kw)``: update many documents by query;
    - ``Bulk.find(query).upsert().update(**kw)``: upsert document;
    - ``Bulk.find(query).remove(**kw)``: remove documents;


1.1.3 (2016-07-23)
==================

* Add ``QuerySet.ids`` method for get only documents id's from queryset;

* Add ``Money.total_cents`` method and ``Money.from_cents`` classmethod;


1.1 (2016-04-26)
================

* Add cacheing on queryset level and use it for ``ReferenceField``;

* Add mongo aggregation framework support;

* Add ``read_preference`` setting;

* Add ``exc`` argument to ``QuerySet.find_one`` for raise exception if not found;

* Add ``multi`` argument to ``QuerySet.remove``;

* Deprecate ``QuerySet.with_id``;

* Refactoring.


1.0 (2015-11-14)
================

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
