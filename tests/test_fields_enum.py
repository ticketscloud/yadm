from enum import Enum

import pytest

from yadm import fields
from yadm.fields.enum import EnumStateSetError
from yadm.documents import Document


class WordsEnum(Enum):
    a = 1
    b = 2
    c = 3


class TestEnumField:
    class Doc(Document):
        __collection__ = 'docs'
        e = fields.EnumField(WordsEnum)

    def test_init(self):
        doc = self.Doc()
        assert not hasattr(doc, 'e')

    def test_save(self, db):
        doc = self.Doc()
        doc.e = WordsEnum.a
        assert doc.e == WordsEnum.a

        db.save(doc)
        raw = db.db['docs'].find_one()
        assert raw['e'] == 1

    def test_load(self, db):
        db.db['docs'].insert_one({'e': 2})
        doc = db(self.Doc).find_one()
        assert isinstance(doc.e, WordsEnum)
        assert doc.e == WordsEnum.b

    def test_set_valid_data(self):
        doc = self.Doc()
        doc.e = WordsEnum.a.value
        assert doc.e == WordsEnum.a

    def test_set_not_valid_data(self):
        doc = self.Doc()
        with pytest.raises(ValueError):
            doc.e = 'wrong'


class TestEnumStateField:
    class Doc(Document):
        __collection__ = 'docs'
        RULES = {
            WordsEnum.a: [WordsEnum.b],
            WordsEnum.b: [WordsEnum.c, WordsEnum.a],
        }
        e = fields.EnumStateField(WordsEnum, rules=RULES, start=WordsEnum.a)

    def test_init(self):
        doc = self.Doc()
        assert doc.e == WordsEnum.a

    def test_init__empty_rules(self):
        with pytest.raises(TypeError):
            fields.EnumField(WordsEnum, rules=[])

    def test_ok(self):
        doc = self.Doc()
        assert doc.e == WordsEnum.a

        doc.e = WordsEnum.b
        assert doc.e == WordsEnum.b

        doc.e = WordsEnum.c
        assert doc.e == WordsEnum.c

    def test_not_ok(self):
        doc = self.Doc()
        assert doc.e == WordsEnum.a

        with pytest.raises(EnumStateSetError):
            doc.e = WordsEnum.c

    def test_set_equal(self):
        doc = self.Doc()
        assert doc.e == WordsEnum.a

        doc.e = WordsEnum.a
        assert doc.e == WordsEnum.a
