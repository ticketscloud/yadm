===========================
Yet Another Document Mapper
===========================

.. image:: https://travis-ci.org/zzzsochi/yadm.svg?branch=master
    :target: https://travis-ci.org/zzzsochi/yadm

It's small and simple ODM for use with MongoDB.

-----------
Quick start
-----------

.. code-block:: python

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
    client = pymongo.MongoClient("localhost", 27017)
    db = Database(client, 'test')

    # Insert post to database
    db.insert(post)

    # Query posts
    qs = db.get_queryset(BlogPost).find({'title': {'$regex': '^s.*'}})
    assert qs.count() > 0

    for post in qs:
        assert post.title.startswith('s')

    # Query one post
    post = db.get_queryset(BlogPost).find_one({'title': 'Small post'})

    # Change post
    post.title = 'Bla-bla-bla title'

    # Save changed post
    db.save(post)


CHANGES
=======

1.1.3 (2016-07-23)
------------------

* Add :py:meth:`QuerySet.ids <yadm.queryset.QuerySet.ids>`
    method for get only documents id's from queryset;

* Add :py:meth:`Money.total_cents <yadm.fields.money.Money.total_cents>`
    method and :py:meth:`Money.total_cents <yadm.fields.money.Money.from_cents>`
    classmethod;


1.1 (2016-04-26)
----------------

* Add cacheing on queryset level and use it for
    :py:class:`ReferenceField <yadm.fields.reference.ReferenceField>`;

* Add mongo aggregation framework support;

* Add ``exc`` argument to
    :py:meth:`QuerySet.find_one <yadm.queryset.QuerySet.find_one>`
    for raise specified exception if not found;

* Add ``multi`` argument to
    :py:meth:`QuerySet.remove <yadm.queryset.QuerySet.remove>`;

* Deprecate :py:meth:`QuerySet.find_one <yadm.queryset.QuerySet.with_id>`

* Refactoring.


1.0 (2015-11-14)
----------------

* Change document structure. No more bad :py:attr:`BaseDocument.__data__ <yadm.documents.BaseDocument.__data__>` attribute:
    - :py:attr:`BaseDocument.__raw__ <yadm.documents.BaseDocument.__raw__>`: raw data from mongo;
    - :py:attr:`BaseDocument.__cache__ <yadm.documents.BaseDocument.__cache__>`: cached objects, casted with fields;
    - :py:attr:`BaseDocument.__changed__ <yadm.documents.BaseDocument.__changed__>`: changed objects.

* Changes api for custom fields:
    - Not more need create field descriptors for every field;
    - :py:meth:`prepare_value <yadm.fields.base.Field.prepare_value>` called only for setattr;
    - :py:meth:`to_mongo <yadm.fields.base.Field.to_mongo>` called only for save objects to mongo;
    - :py:meth:`from_mongo <yadm.fields.base.Field.from_mongo>` called only for load values from :py:attr:`BaseDocument.__raw__ <yadm.documents.BaseDocument.__raw__>`;
    - Remove `Field.default` attribute. Use :py:meth:`Field.get_default <yadm.fields.base.Field.get_default>` method;
    - Add :py:meth:`get_if_not_loaded <yadm.fields.base.Field.get_if_not_loaded>` and :py:meth:`get_if_attribute_not_set <yadm.fields.base.Field.get_if_attribute_not_set>` method;
    - By default raise :py:class:`NotLoadedError <yadm.fields.base.NotLoadedError>` if field not loaded from projection;

* Changes in :py:class:`ReferenceField <yadm.fields.reference.ReferenceField>`:
    - Raise :py:class:`BrokenReference <yadm.fields.reference.BrokenReference>` if link is bloken;
    - Raise :py:class:`NotBindingToDatabase <yadm.fields.reference.NotBindingToDatabase>` if document not saved to database;

* `smart_null` keyword for :py:class:`Field <yadm.fields.base.Field>`;

* Fields in document must be instances (not classes!);

* Remove `ArrayContainer` and `ArrayContainerField`;

* Remove old `MapIntKeysField` and `MapObjectIdKeysField`. Use new :py:class:`MapCustomKeysField <yadm.fields.map.MapCustomKeysField>`;

* Add :py:meth:`Database.update_one <yadm.database.Database.update_one>` method for run simple update query with specified document;

* Add :py:meth:`QuerySet.distinct <yadm.queryset.QuerySet.distinct>`;

* :py:func:`serialize.from_mongo <yadm.serialize.from_mongo>` now accept `not_loaded` sequence with filed names who must mark as not loaded, `parent` and `name`;

* :py:func:`serialize.to_mongo <yadm.serialize.to_mongo>` do not call :py:meth:`FieldDescriptor.__set__ <yadm.fields.base.FieldDescriptor.__set__>`;

* Fakers! Subsystem for generate test objects;

* Tests now use pytest;

* And more, and more...


-----------------
API documentation
-----------------

.. toctree::
   :maxdepth: 5

   api/index
