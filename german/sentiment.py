# Source of sentiment analysis http://wortschatz.uni-leipzig.de/de/download#sentiWSDownload
import itertools
from csv import reader
from os.path import join, dirname

pos_file = join(dirname(__file__), "SentiWS_v1.8c_Positive.txt")
neg_file = join(dirname(__file__), "SentiWS_v1.8c_Negative.txt")


def load_sentiment(file):
    """
    Loads the sentiment file and processes it. The given sentiment files have ADJX as a short form for both ADJA and
    ADJD, which is lifted. The scores are assigned to the base forms and all alternative forms as well. They are mapped
    to lower case.
    :param file: The file to load from, either pos_file or neg_file.
    :return: Returns the sentiment data as as a dictionary (lower case word, POS) to score.
    """

    def primary(row):
        return row[0].split('|')[0]

    def poss(row):
        pos = row[0].split('|')[1]
        return ["ADJA", "ADJD"] if pos == "ADJX" else [pos]

    def score(row):
        return float(row[1])

    def alternative(row):
        return [] if len(row) <= 2 else row[2].split(',')

    def forms(row):
        return itertools.chain([primary(row)], alternative(row))

    with open(file, 'r', encoding="utf-8") as h:
        rows = reader(h, delimiter='\t')
        return dict(((form.lower(), pos), score(row)) for row in rows for form in forms(row) for pos in poss(row))


# Load individual files
pos = load_sentiment(pos_file)
neg = load_sentiment(neg_file)


def sentiment_german(sentence_tagged):
    """
    Generates a score for the given sentence.
    :param sentence_tagged: A list of words and their pos tags, as (word, POS)
    :return: Returns the score values as (sum positive, sum_negative, n)
    """
    # Get scores base on word and POS tag.
    scores_pos = list(pos.get((word.lower(), tag), 0.0) for (word, tag) in sentence_tagged)
    scores_neg = list(neg.get((word.lower(), tag), 0.0) for (word, tag) in sentence_tagged)

    # Return sum, word count, min and max.
    return sum(scores_pos), sum(scores_neg), len(scores_pos)
