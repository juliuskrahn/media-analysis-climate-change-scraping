from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "TheTimesOfIndia"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    n = 42005
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield f"https://timesofindia.indiatimes.com/{date.year}/{date.month}/{date.day}/archivelist/" \
              f"year-{date.year},month-{date.month},starttime-{n}.cms"
        n += 1


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"/([0-9]*)/([0-9]*)/([0-9]*)", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  int(resp_url_re_result.group(2)),
                                  int(resp_url_re_result.group(3)))
    soup = BeautifulSoup(resp.text, "lxml")
    container = soup.find("span", attrs={"style": "font-family:arial ;font-size:12;color: #006699"})
    try:
        for el in container.find_all("a"):
            yield el.get("href"), PUBLISHER, el.string, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
