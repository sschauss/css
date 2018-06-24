from iso8601 import parse_date
from pyspark.sql import SparkSession, Row

from german.postag import postag_german
from german.punkt import sentences_german
from german.sentiment import sentiment_german
from spiegel import min_date

executor_count = 4
sample_fraction = 1


def month_num(date):
    """
    Returns the month number of the date from the minimum date.
    :param date: The date to get the month number from.
    :return: Returns a positive number.
    """
    if type(date) is str:
        date = parse_date(date)
    return (date.year - min_date.year) * 12 + date.month - min_date.month


def unfold(sentiment):
    """
    Unfolds the sentiment into a tuple of count, sum positive, sum negative, sum, volume, min and max.
    :param sentiment: The sentiment to unfold.
    :return: A tuple of (count, sum positive, sum negative, sum, volume, min and max).
    """
    sum_pos, sum_neg, n = sentiment
    return 1, sum_pos, sum_neg, sum_pos + sum_neg, n, sum_pos + sum_neg, sum_pos + sum_neg


def merge(a, b):
    """
    Merges to unfolded tuples. Count, sum and volume are summed, min and max are extended respectively.
    :param a: The first unfolded sentiment.
    :param b: The second unfolded sentiment.
    :return: Returns a new unfolded tuple.
    """
    count_a, sum_pos_a, sum_neg_a, sum_a, n_a, min_a, max_a = a
    count_b, sum_pos_b, sum_neg_b, sum_b, n_b, min_b, max_b = b

    return count_a + count_b, \
           sum_pos_a + sum_pos_b, \
           sum_neg_a + sum_neg_b, \
           sum_a + sum_b, \
           n_a + n_b, \
           min(min_a, min_b), \
           max(max_a, max_b)


if __name__ == '__main__':
    spark = SparkSession.builder.appName('analyzer').master('local[{}]'.format(executor_count)).getOrCreate()

    spark \
        .read \
        .csv("articles-csv", header=True) \
        .rdd \
        .filter(lambda row: type(row.article) is str) \
        .flatMap(lambda row: [Row(date=row.date,
                                  article_url=row.article_url,
                                  sentence=s) for s in sentences_german(row.article)]) \
        .filter(lambda row: "flücht" in row.sentence.lower()) \
        .map(lambda row: (month_num(row.date), unfold(sentiment_german(postag_german(row.sentence))))) \
        .reduceByKey(merge) \
        .map(
        lambda data: Row(month=data[0],
                         count=data[1][0],
                         sum_pos=data[1][1],
                         sum_neg=data[1][2],
                         sum=data[1][3],
                         n=data[1][4],
                         min=data[1][5],
                         max=data[1][6])) \
        .toDF() \
        .coalesce(1) \
        .orderBy("month") \
        .write \
        .format('csv') \
        .mode('overwrite') \
        .option("header", "true") \
        .save('sentiments-csv')
