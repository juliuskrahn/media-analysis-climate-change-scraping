from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import re
import logging


PUBLISHER = "NewsComAu"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield "https://www.news.com.au/sitemap.xml?" + date.strftime("yyyy=%Y&mm=%m&dd=%d")


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"yyyy=([0-9]{4})&mm=([0-9]{2})&dd=([0-9]{2})", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  int(resp_url_re_result.group(2)),
                                  int(resp_url_re_result.group(3)))

    soup = BeautifulSoup(resp.text, "xml")
    try:
        for el in soup.find_all("url"):
            url = el.find("loc").string
            yield url, PUBLISHER, None, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
