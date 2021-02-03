from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "TheTimes"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for year in range(2015, 2021):
        for month in range(1, 13):
            root_url = f"https://www.thetimes.co.uk/html-sitemap/{year}-{month:0>2}-"
            for week in range(1, 5):
                yield root_url + str(week)


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    url_re_result = re.search(r"/([0-9]{4})-([0-9]{2})-([0-9])", resp.url)
    published = datetime.datetime(int(url_re_result.group(1)),
                                  int(url_re_result.group(2)),
                                  int(url_re_result.group(3))*7-6)
    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.find_all(class_="Sitemap-link"):
            yield "https://www.thetimes.co.uk" + el.a.attrs.get("href"), PUBLISHER, el.a.string, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
