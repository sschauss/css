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

# Combine dictionaries
combined = pos.copy()
combined.update(neg)


def sentiment_german(sentence_tagged):
    """
    Generates a score for the given sentence.
    :param sentence_tagged: A list of words and their pos tags, as (word, POS)
    :return: Returns the score values as (sum, n)
    """
    # Get scores base on word and POS tag.
    scores = list(combined.get((word.lower(), tag), 0.0) for (word, tag) in sentence_tagged)

    # Return sum, word count, min and max.
    return sum(scores), len(scores)

# if __name__ == '__main__':
#     with open("../articles/articles-csv/part-00071-41da5c05-2d0c-46f7-8914-cc50230d2634-c000.csv", "r",
#               encoding="utf-8") as h:
#         articles = reader(h, doublequote=False, escapechar="\\")
#         next(articles, None)
#         for article in articles:
#             print("======================================")
#             total = 0
#             for sentence in sentences_german(article[0]):
#                 tagged = postag_german(sentence)
#                 analysed = sentiment_german(tagged)
#                 print("%f: %s" % (analysed[0], sentence))
#                 total += analysed[0]
#             print("Overall: %f" % total)
#             print()
