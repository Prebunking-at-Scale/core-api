import langid


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
    return predictions[0]
