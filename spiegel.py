from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from bs4 import BeautifulSoup, Comment
from functools import reduce
from common import download
from traceback import format_exc
import re
import math

min_date = datetime(2000, 1, 1)
base_url = 'http://www.spiegel.de'
archive_url_template = base_url + '/nachrichtenarchiv/artikel-{}.html'

executor_count = 64
sample_fraction = 1
spark = SparkSession.builder.appName('scraper').master('local[{}]'.format(executor_count)).getOrCreate()


def build_archive_url(date):
    return archive_url_template.format(date.strftime('%d.%m.%Y'))


def generate_urls(min_date):
    delta = datetime.today() - min_date
    return [build_archive_url(min_date + timedelta(days=n)) for n in range(delta.days)]


def extract_article_urls(url):
    html = download(url)
    if html is not None:
        return [a_tag['href'] if re.match('http.*', a_tag['href']) else base_url + a_tag['href'] for a_tag in
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
            plus_selector = '.article-icon.spiegelplus'
            if soup.select_one(plus_selector) is not None:
                return None
            else:
                content_main = soup.select_one('#content-main')
                selectors_to_remove = ['.article-function-social-media',
                                       '.article-function-box',
                                       'script',
                                       'style',
                                       '#js-article-column > p',
                                       '#js-article-top-wide-asset',
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
                for comment in soup.findAll(text=lambda text: isinstance(text, Comment)):
                    comment.extract()
                content = re.sub('(\r\n|\n|\t|\s+)', ' ',
                                 reduce(lambda agg, cur: agg + ' ' + cur, content_main.findAll(text=True)))
                return url, content
    except Exception as e:
        print('extraction of {} failed ({})'.format(url, format_exc()))


if __name__ == '__main__':
    urls = generate_urls(min_date)
    article_urls = spark \
        .sparkContext \
        .parallelize(urls) \
        .sample(fraction=sample_fraction, withReplacement=False) \
        .flatMap(lambda archive_url: extract_article_urls(archive_url)) \
        .filter(lambda article_url: 'spiegel.de' in article_url) \
        .filter(lambda article_url: 'spiegel.de/video' not in article_url) \
        .cache()

    article_urls \
        .map(lambda article_url: (article_url, )) \
        .toDF(['url']) \
        .write \
        .format('csv') \
        .mode('overwrite') \
        .save('article_urls-csv')

    article_url_count = article_urls.count()

    print('article url count: {}'.format(article_url_count))

    article_urls \
        .repartition(max(executor_count, math.ceil(article_url_count / 512.0))) \
        .map(lambda article_url: extract_article_content(article_url)) \
        .filter(lambda article: article is not None) \
        .toDF(['url', 'article']) \
        .write \
        .format('csv') \
        .mode('overwrite') \
        .save('articles-csv')
