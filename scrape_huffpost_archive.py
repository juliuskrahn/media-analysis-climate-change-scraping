from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "Huffpost"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield "https://www.huffpost.com/archive/" + date.strftime("%Y-%m-%d")


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"/([0-9]{4})-([0-9]{2})-([0-9]{2})", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  int(resp_url_re_result.group(2)),
                                  int(resp_url_re_result.group(3)))
    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.select(".archive__entries .card"):
            headline = el.find(class_="card__link yr-card-headline")
            url = headline.attrs.get("href")
            title = headline.find(class_="card__headline__text").get_text().strip()
            try:
                category = el.find(class_="card__label__text").string
            except AttributeError:
                category = None
            yield url, PUBLISHER, title, published, category, None

    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
