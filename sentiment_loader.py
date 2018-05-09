from csv import reader
from itertools import chain

def load_sentiment_file(file):
    def primary(row):
        return row[0].split('|')[0]

    def poss(row):
        pos = row[0].split('|')[1]
        return ['ADJA', 'ADJD'] if pos == 'ADJX' else [pos]

    def score(row):
        return float(row[1])

    def alternative(row):
        return [] if len(row) <= 2 else row[2].split(',')

    def forms(row):
        return chain([primary(row)], alternative(row))

    with open(file, 'r', encoding="utf-8") as h:
        rows = reader(h, delimiter='\t')
        return dict(((form.lower(), pos), score(row)) for row in rows for form in forms(row) for pos in poss(row))