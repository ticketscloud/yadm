import pytest

from yadm import fields
from yadm.documents import Document
from yadm.testing import create_fake


class TestStaticField:
    class Doc(Document):
        __collection__ = 'docs'

        int = fields.StaticField(13)
        str = fields.StaticField('string')

    def test_init(self):
        doc = self.Doc()

        assert doc.int == 13
        assert doc.str == 'string'

    def test_set(self):
        doc = self.Doc()

        with pytest.raises(AttributeError):
            doc.int = 26

        with pytest.raises(AttributeError):
            doc.int = 13

        with pytest.raises(AttributeError):
            doc.str = ''

        with pytest.raises(AttributeError):
            doc.str = None

    def test_save(self, db):
        doc = self.Doc()
        db.save(doc)

        raw = db.db['docs'].find_one()

        assert raw['int'] == 13
        assert raw['str'] == 'string'

    def test_load(self, db):
        db.db['docs'].insert_one({'int': 13, 'str': 'string'})

        doc = db(self.Doc).find_one()

        assert doc.int == 13
        assert doc.str == 'string'

    @pytest.mark.parametrize('data, field', [
        ({'int': 26, 'str': 'string'}, 'int'),
        ({'int': 13, 'str': 'bad string'}, 'str'),
        ({'int': None, 'str': 'string'}, 'int'),
        ({'str': 'string'}, 'int'),
    ])
    def test_bad_data_in_db(self, db, data, field):
        db.db['docs'].insert_one(data)

        doc = db(self.Doc).find_one()

        with pytest.raises(RuntimeError):
            getattr(doc, field)

    def test_faker(self):
        doc = create_fake(self.Doc)
        assert doc.int == 13
        assert doc.str == 'string'

        with pytest.raises(AttributeError):
            doc.int = 666

    def test_faker_save(self, db):
        doc = create_fake(self.Doc, db)
        assert doc.int == 13
        assert doc.str == 'string'

        raw = db.db[self.Doc.__collection__].find_one()
        assert raw['int'] == 13
        assert raw['str'] == 'string'
