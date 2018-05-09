from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from urllib.request import urlopen
from traceback import format_exc
from bs4 import BeautifulSoup

min_date = datetime(2000, 1, 1)
baseUrl = 'http://www.spiegel.de/nachrichtenarchiv/artikel-{}.html'

spark = SparkSession.builder.appName('scraper').master('local[64]').getOrCreate()


def build_url(date):
    return baseUrl.format(date.strftime('%d.%m.%Y'))


def generate_urls(min_date):
    delta = datetime.today() - min_date
    return [build_url(min_date + timedelta(days=n)) for n in range(delta.days)]


def download(url):
    try:
        return urlopen(url).read().decode()
    except Exception as e:
        print('download of {} failed ({})'.format(url, format_exc()))


def extract_links(url):
    html = download(url)
    if html is not None:
        return [a_tag['href'] for a_tag in BeautifulSoup(html, 'lxml').select('#content-main .column-wide ul li a')]
    else:
        return []


if __name__ == '__main__':
    urls = generate_urls(min_date)
    spark \
        .sparkContext \
        .parallelize(urls) \
        .repartition(256) \
        .flatMap(lambda url: extract_links(url)) \
        .map(lambda e: (e,)) \
        .toDF() \
        .write \
        .format("csv") \
        .save("links.csv")
