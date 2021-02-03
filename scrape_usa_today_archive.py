from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging
import calendar


PUBLISHER = "USAToday"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield f"https://eu.usatoday.com/sitemap/{date.year}/{calendar.month_name[date.month].lower()}/{date.day}/"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"/([0-9]{4})/(.*)/([0-9]*)/", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  list(calendar.month_name).index(resp_url_re_result.group(2).capitalize()),
                                  int(resp_url_re_result.group(3)))

    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.select(".sitemap-column-wrapper a"):
            yield el.get("href"), PUBLISHER, el.string, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
