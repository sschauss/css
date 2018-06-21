import warnings

from pattern.text.de import parse

from german.punkt import sentences_german


def postag_german(sentence):
    """
    Returns for a sentence a list of tuples of (word, POS).
    :param sentence: The sentence to tag.
    :return: A list of tuples.
    """
    # Parse the sentence, target POS tags are STTS, do not do chunk parts and relations, use list of words and
    # POS tags instead of complex tree. Base tokenization from library is applied but not relied on, as it just does
    # simple character based spitting.
    parsed = parse(sentence, tagset='STTS', chunks=False, relations=False, split=True)

    # No parsed result, unrecoverable exception.
    if len(parsed) == 0:
        raise Exception('Sentence for POS tagging parsed as multiple sentences.')

    # More than one parsed result, ok, take first.
    if len(parsed) > 1:
        warnings.warn("Sentence for POS tagging parsed as multiple sentences.")

    # Return tuples of words and POS tags from parsed result.
    return list((word[0], word[1]) for word in parsed[0])


if __name__ == '__main__':
    # Source cites POS tags as STTS tags http://wortschatz.uni-leipzig.de/de/download#sentiWSDownload
    source = "Die Katze liegt auf der Matte. Sie ist aus Gold gemacht."
    sentences = sentences_german(source)
    for s in sentences:
        print(postag_german(s))
