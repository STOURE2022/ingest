import pytest
from src.utils import parse_bool, normalize_delimiter


def test_parse_bool():
    assert parse_bool("true") == True
    assert parse_bool("0") == False
    assert parse_bool(None, default=True) == True


def test_normalize_delimiter():
    assert normalize_delimiter(",") == ","
    assert normalize_delimiter(";") == ";"
    with pytest.raises(ValueError):
        normalize_delimiter(";;")
