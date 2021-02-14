from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import re
import logging


PUBLISHER = "VG"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for year in range(2015, 2021):
        for month in range(1, 13):
            yield f"https://www.vg.no/sitemap/files/{year}{month:0>2}-articles.xml.gz"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"/([0-9]{4})([0-9]{2})", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  int(resp_url_re_result.group(2)),
                                  1)

    soup = BeautifulSoup(resp.text, "xml")
    try:
        for el in soup.find_all("url"):
            url = el.find("loc").string
            yield url, PUBLISHER, None, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
