from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "TheNYTimes"


# Got always blocked !!!


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for year in range(2015, 2021):
        for month in range(1, 13):
            root_url = f"https://spiderbites.nytimes.com/{year}/articles_{year}_{month:0>2}_"
            for page in range(6):
                yield f"{root_url}{page:0>5}.html"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=False)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.select("#headlines li"):
            url = el.a.attrs.get("href")
            url_re_result = re.search(r"/([0-9]{4})/([0-9]{2})/([0-9]{2})", resp.url)
            published = datetime.datetime(int(url_re_result.group(1)),
                                          int(url_re_result.group(2)),
                                          int(url_re_result.group(3)),
                                          0,
                                          0)
            yield url, PUBLISHER, el.a.string, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
