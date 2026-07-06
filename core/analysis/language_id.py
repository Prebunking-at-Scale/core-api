import langid
import pycountry


def normalize_language_code(code: str | None) -> str | None:
    """
    Normalise a language value to a two letter ISO 639-1 code.

    Accepts two/three letter codes or language names (e.g. "eng", "English")
    and returns the matching two letter code (e.g. "en"). The original value,
    lowercased, is returned if it cannot be resolved or has no two letter code.
    """
    if not code:
        return code
    code = code.strip()

    # An exact two letter match takes priority: pycountry's fuzzy lookup can
    # otherwise match a two letter input to an unrelated language name.
    if len(code) == 2 and pycountry.languages.get(alpha_2=code.lower()):
        return code.lower()

    try:
        language = pycountry.languages.lookup(code)
    except LookupError:
        return code.lower()

    return getattr(language, "alpha_2", None) or code.lower()


def predict_language(text: str) -> str:
    """
    Predicts the main language of a given piece of text.

    Parameters
        ----------
        text : str
            Some text

        Returns
        -------
        str
            The predicted two letter language code.
            For example, "en" for English or "ar" for Arabic.
    """
    predictions = langid.classify(text)
    return normalize_language_code(predictions[0]) or predictions[0]
