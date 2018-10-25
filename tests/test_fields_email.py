import pytest

from yadm.documents import Document
from yadm.fields.email import EmailField, InvalidEmail


class Doc(Document):
    e = EmailField()


def test_ok():
    doc = Doc()
    doc.e = 'E@mA.iL'

    assert doc.e == 'e@ma.il'


@pytest.mark.parametrize('bad_email', ['EmA.iL', 'E@mA@iL', 'EmAiL@'])
def test_error(bad_email):
    doc = Doc()

    with pytest.raises(InvalidEmail):
        doc.e = bad_email
