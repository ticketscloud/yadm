from bson import ObjectId

from yadm.documents import Document
from yadm.fields import StringField, ReferenceField
from yadm.serialize import LOOKUPS_KEY


class RefDoc(Document):
    __collection__ = 'refs'

    s = StringField()


class Doc(Document):
    __collection__ = 'docs'

    s = StringField()
    ref = ReferenceField(RefDoc)


def test_lookup(db):
    for n in range(10):
        db.db['docs'].insert_one({
            's': 'doc-{}'.format(n),
            'ref': db.db['refs'].insert_one({'s': 'ref-{}'.format(n)}).inserted_id,
        })

    qs = db(Doc).lookup('ref')
    docs = [d for d in qs]

    assert len(docs) == 10

    for doc in docs:
        assert isinstance(doc.__raw__['ref'], ObjectId)
        assert LOOKUPS_KEY not in doc.__raw__
        assert doc.__yadm_lookups__['ref']['s'] == 'ref-{}'.format(doc.s.split('-')[1])
        assert doc.ref.s == 'ref-{}'.format(doc.s.split('-')[1])

    for doc in docs:
        doc.__cache__.clear()
        doc.__db__ = None
        doc.__qs__ = None

    for doc in docs:
        assert doc.ref.s == 'ref-{}'.format(doc.s.split('-')[1])
