import pytest
from bson import ObjectId

from yadm.documents import Document
from yadm.log_items import Save, SetField
from yadm.markers import AttributeNotSet
from yadm.exceptions import NotLoadedError
from yadm import fields


class Doc(Document):
    __collection__ = 'testdocs'
    i = fields.IntegerField()
    b = fields.BooleanField()


@pytest.fixture
def doc():
    return Doc()


def test__db(doc):
    assert doc.__db__ is None


def test_fields():
    assert set(Doc.__fields__) == {'_id', 'i', 'b'}


def test_inheritance_fields():
    class InhDoc(Doc):
        d = fields.DecimalField()

    assert set(InhDoc.__fields__) == {'_id', 'i', 'b', 'd'}
    assert set(Doc.__fields__) == {'_id', 'i', 'b'}
    assert Doc.__fields__['i'] is not InhDoc.__fields__['i']
    assert Doc.__fields__['i'].document_class is Doc
    assert InhDoc.__fields__['i'].document_class is InhDoc


def test_raw_cache_log(db, doc):
    assert not doc.__raw__
    assert not doc.__log__
    assert doc.__cache__ == {'_id': AttributeNotSet,
                             'i': AttributeNotSet,
                             'b': AttributeNotSet}

    doc.i = 13

    assert not doc.__raw__
    assert len(doc.__log__) == 1
    assert doc.__log__[0] == SetField(name='i', value=13)
    assert doc.__cache__ == {'_id': AttributeNotSet,
                             'i': 13,
                             'b': AttributeNotSet}

    db.save(doc)

    assert not doc.__raw__
    assert len(doc.__log__) == 3
    assert doc.__log__[1] == SetField(name='_id', value=doc.id)
    assert doc.__log__[2] == Save(id=doc.id)
    assert doc.__cache__ == {'_id': doc.id,
                             'i': 13,
                             'b': AttributeNotSet}

    doc = db.reload(doc, new_instance=True)

    assert doc.__raw__ == {'i': 13, '_id': doc.id}
    assert len(doc.__log__) == 0  # reload with new_instance=True
    assert doc.__cache__ == {'_id': doc.id}

    doc.b = False

    assert doc.__raw__ == {'i': 13, '_id': doc.id}
    assert doc.__cache__ == {'b': False, '_id': doc.id}

    assert doc.i == 13  # call descriptor's get

    assert doc.__raw__ == {'i': 13, '_id': doc.id}
    assert doc.__cache__ == {'i': 13, 'b': False, '_id': doc.id}

    doc.i = 12  # call descriptor's set, for cover

    assert doc.__raw__ == {'i': 13, '_id': doc.id}
    assert doc.__cache__ == {'i': 12, 'b': False, '_id': doc.id}


def test_eq():
    doc_a = Doc()
    doc_a.id = ObjectId()

    doc_b = Doc()
    doc_b.id = ObjectId()

    doc_c = Doc()
    doc_c.id = doc_a.id

    assert doc_a != doc_b
    assert doc_a == doc_c


def test_id(doc):
    with pytest.raises(AttributeError):
        doc.id


def test_attributeerror(db):
    _id = db.db.testdocs.insert_one({'i': 13}).inserted_id
    doc = db(Doc).find_one(_id)

    assert doc.i == 13

    with pytest.raises(AttributeError):
        doc.b


def test_notloaded(db):
    _id = db.db.testdocs.insert_one({'i': 13, 'b': False}).inserted_id
    doc = db(Doc).fields('i').find_one(_id)

    assert doc.i == 13

    with pytest.raises(NotLoadedError):
        doc.b


def test_field_not_instant():
    with pytest.raises(TypeError):
        class Doc(Document):
            field = fields.Field


def test_instant_document_errors():
    with pytest.raises(TypeError):
        Doc({}, {})  # a lot of positional arguments

    with pytest.raises(TypeError):
        Doc(1)  # not a dict


def test_str():
    doc = Doc()
    assert str(doc)
    assert str(doc) == repr(doc)
