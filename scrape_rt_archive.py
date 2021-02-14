from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "RT"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for year in range(2015, 2021):
        yield f"https://www.rt.com/sitemap_{year}.xml"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "xml")
    try:
        for el in soup.find_all("url"):
            url = el.find("loc").string
            published = datetime.datetime.fromisoformat(el.lastmod.string)
            yield url, PUBLISHER, None, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
