from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "ABSCBNNews"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield "https://news.abs-cbn.com/sitemaps/news/sitemap-articles-" + date.strftime("%Y-%m") + ".xml"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "xml")
    try:
        for el in soup.find_all("url"):
            url = el.find("loc").string
            published = datetime.datetime.fromisoformat(el.find("news:publication_date").string)
            title = el.find("news:title").string
            yield url, PUBLISHER, title, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
