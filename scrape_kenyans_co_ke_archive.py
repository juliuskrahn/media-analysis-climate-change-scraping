from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "KenyansCoKe"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for page in range(1, 25):
        yield "https://www.kenyans.co.ke/sitemap.xml?page=" + str(page)


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "xml")
    try:
        for el in soup.find_all("url"):
            if el.priority.string != "1.0":
                continue
            url = el.loc.string
            published = datetime.datetime.fromisoformat(el.lastmod.string)
            yield url, PUBLISHER, None, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
