from bs4 import BeautifulSoup
import datetime
from time import strptime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "TheGuardian"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    categories = [
        "world",
        "uk-news",
        "uk/environment",
        "science",
        "global-development",
        "football",
        "uk/technology",
        "uk/business",
        "tone/editorials",
        "cartoons/archive",
        "food",
        "tone/recipes",
        "uk/travel",
        "uk/money"
    ]
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        for category in categories:
            yield f"https://www.theguardian.com/{category}/{date.strftime('%Y/%b/%d')}/all"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"/([0-9]{4})/(.*)/([0-9]{2})", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  strptime(resp_url_re_result.group(2), "%b").tm_mon,
                                  int(resp_url_re_result.group(3)))

    soup = BeautifulSoup(resp.text, "lxml")
    category_regexp = re.compile(r"theguardian\.com/(.*)/[0-9]{4}")
    for el in soup.select(".u-faux-block-link__overlay.js-headline-text"):
        try:
            url = el.attrs.get("href")
            category = category_regexp.search(url).group(1)
            yield url, PUBLISHER, el.string, published, category, None
        except AttributeError:
            continue
