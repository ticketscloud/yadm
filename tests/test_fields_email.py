import pytest

from yadm.documents import Document
from yadm.fields.email import EmailField, InvalidEmail


class Doc(Document):
    e = EmailField()


def test_ok():
    doc = Doc()
    doc.e = 'E@mA.iL'

    assert doc.e == 'e@ma.il'


def test_error():
    doc = Doc()

    with pytest.raises(InvalidEmail):
        doc.e = 'EmA.iL'
