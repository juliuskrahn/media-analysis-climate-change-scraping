from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "CNN"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for year in range(2015, 2021):
        for month in range(1, 13):
            yield f"https://edition.cnn.com/article/sitemap-{year}-{month}.html"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.select(".sitemap-entry li"):
            try:
                url = el.a.attrs.get("href")
                published = datetime.datetime.fromisoformat(el.find("span", class_="date").string)
                title = el.a.string
                yield url, PUBLISHER, title, published, None, None
            except AttributeError:
                continue
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
