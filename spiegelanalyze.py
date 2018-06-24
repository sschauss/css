from iso8601 import parse_date
from pyspark.sql import SparkSession, Row

from german.postag import postag_german
from german.punkt import sentences_german
from german.sentiment import sentiment_german
from spiegel import min_date

executor_count = 4
sample_fraction = 1
spark = SparkSession.builder.appName('analyzer').master('local[{}]'.format(executor_count)).getOrCreate()


def week_since_origin(date):
    yb, wb, _ = min_date.isocalendar()
    yt, wt, _ = parse_date(date).isocalendar()
    return (yt - yb) * 52 + wt - wb


def unfold_sentiment(sentiment):
    sum, n = sentiment
    return sum, n, sum, sum


def merge_sentiment(a, b):
    sum_a, n_a, min_a, max_a = a
    sum_b, n_b, min_b, max_b = b
    return sum_a + sum_b, n_a + n_b, min(min_a, min_b), max(max_a, max_b)


if __name__ == '__main__':
    some = spark \
        .read \
        .csv("articles-csv", header=True) \
        .rdd \
        .filter(lambda row: type(row.article) is str) \
        .flatMap(lambda row: [Row(date=row.date,
                                  article_url=row.article_url,
                                  sentence=s) for s in sentences_german(row.article)]) \
        .filter(lambda row: "flücht" in row.sentence.lower()) \
        .map(lambda row: (week_since_origin(row.date), unfold_sentiment(sentiment_german(postag_german(row.sentence))))) \
        .reduceByKey(merge_sentiment) \
        .map(lambda data: Row(week=data[0], sum=data[1][0], n=data[1][1], min=data[1][2], max=data[1][3])) \
        .toDF() \
        .coalesce(1) \
        .orderBy("week") \
        .write \
        .format('csv') \
        .mode('overwrite') \
        .option("header", "true") \
        .save('sentiments-csv')
