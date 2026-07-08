import pytest

from core.analysis.language_id import normalize_language_code, predict_language


@pytest.mark.parametrize(
    "value,expected",
    [
        ("en", "en"),
        ("eng", "en"),
        ("ENG", "en"),
        ("English", "en"),
        (" en ", "en"),
        ("EN", "en"),
        ("spa", "es"),
        ("es", "es"),
        ("fra", "fr"),
        ("fre", "fr"),
        ("zho", "zh"),
        ("chi", "zh"),
        ("arabic", "ar"),
    ],
)
def test_normalize_language_code_resolves_to_two_letter(value, expected):
    assert normalize_language_code(value) == expected


@pytest.mark.parametrize("value", [None, ""])
def test_normalize_language_code_passes_through_empty(value):
    assert normalize_language_code(value) == value


def test_normalize_language_code_keeps_unknown_value_lowercased():
    assert normalize_language_code("XX") == "xx"


def test_predict_language_returns_two_letter_code():
    assert predict_language("This is a sentence written in English.") == "en"
