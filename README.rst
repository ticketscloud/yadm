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

::

    import pymongo
    from yadm import Database, Document, fields


    # Create model
    class BlogPost(Document):
        __collection__ = 'blog_posts'

        title = fields.StringField
        body = fields.StringField


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
    qs = db.get_queryset(BlogPost).find({'title': {'$regex': '^S'}})
    assert qs.count() > 0

    for post in qs:
        assert post.title.startswith('S')

    # Query one post
    post = db.get_queryset(BlogPost).find_one({'title': 'Small post'})

    # Change post
    post.title = 'Bla-bla-bla title'

    # Save changed post
    db.save(post)
