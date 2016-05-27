import pytest

from yadm.documents import Document
from yadm.fields.email import EmailField, InvalidEmail


class TestDoc(Document):
    e = EmailField()


def test_ok():
    doc = TestDoc()
    doc.e = 'E@mA.iL'

    assert doc.e == 'e@ma.il'


def test_error():
    doc = TestDoc()

    with pytest.raises(InvalidEmail):
        doc.e = 'EmA.iL'
