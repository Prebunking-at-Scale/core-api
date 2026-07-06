import pytest

from core.analysis.genai import Sentence


def _sentence(language: str) -> Sentence:
    # model_validate mirrors how the google-genai SDK builds response.parsed,
    # so this exercises the same path that stores languages on ingest.
    return Sentence.model_validate(
        {
            "text": "hello",
            "source": "audio",
            "start_time_s": 0.0,
            "language": language,
        }
    )


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("en", "en"),
        ("eng", "en"),
        ("ENG", "en"),
        ("English", "en"),
        (" en ", "en"),
        ("spa", "es"),
        ("zho", "zh"),
        ("Armenian", "hy"),
    ],
)
def test_sentence_normalises_language_on_parse(raw, expected):
    assert _sentence(raw).language == expected


def test_sentence_keeps_unknown_language_lowercased():
    assert _sentence("XX").language == "xx"
