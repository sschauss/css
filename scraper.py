from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from urllib.request import urlopen
from traceback import format_exc
from bs4 import BeautifulSoup
from functools import reduce
import re

min_date = datetime(2000, 1, 1)
base_url = 'http://www.spiegel.de'
archive_url_template = base_url + '/nachrichtenarchiv/artikel-{}.html'

spark = SparkSession.builder.appName('scraper').master('local[8]').getOrCreate()


def build_archive_url(date):
    return archive_url_template.format(date.strftime('%d.%m.%Y'))


def generate_urls(min_date):
    delta = datetime.today() - min_date
    return [build_archive_url(min_date + timedelta(days=n)) for n in range(delta.days)]


def download(url):
    try:
        return urlopen(url).read().decode()
    except Exception as e:
        print('download of {} failed ({})'.format(url, format_exc()))


def extract_article_urls(url):
    html = download(url)
    if html is not None:
        return [base_url + a_tag['href'] for a_tag in
                BeautifulSoup(html, 'lxml').select('#content-main .column-wide ul li a')]
    else:
        print('extraction of urls {} failed ({})'.format(url, format_exc()))
        return []


def extract_text(nodes):
    return reduce(lambda agg, cur: agg + cur.getText(), nodes)


def extract_article_content(url):
    html = download(url)
    try:
        if html is not None:
            soup = BeautifulSoup(html, 'lxml')
            content_main = soup.select_one('#content-main')
            selectors_to_remove = ['.article-function-social-media',
                                   '.article-function-box',
                                   'script',
                                   '#js-article-column > p',
                                   '.asset-box',
                                   '.article-copyright',
                                   '.article-function-box-wide',
                                   '.top-anchor',
                                   '.module-box',
                                   '.spiegel-asset-box',
                                   '#spRecommendations',
                                   '#js-video-slider',
                                   '.column-both-bottom',
                                   '#footer']
            for selector in selectors_to_remove:
                for node in content_main.select(selector):
                    node.decompose()
            content = re.sub('[\n\r\t]', '', reduce(lambda agg, cur: agg + ' ' + cur, content_main.findAll(text=True)))
            return (url, content)
    except Exception as e:
        print('extraction of {} failed ({})'.format(url, format_exc()))


if __name__ == '__main__':
    # print(extract_article_urls('http://www.spiegel.de/nachrichtenarchiv/artikel-08.09.2010.html'))
    urls = generate_urls(min_date)
    article_urls = spark \
        .sparkContext \
        .parallelize(urls) \
        .repartition(256) \
        .flatMap(lambda archive_url: extract_article_urls(archive_url))

    article_url_count = article_urls.count()

    article_urls \
        .repartition(article_url_count / 512 + 1) \
        .map(lambda article_url: extract_article_content(article_url)) \
        .filter(lambda article: article is not None) \
        .toDF(['url', 'article']) \
        .write \
        .format("orc") \
        .orc("articles.orc")

