from bs4 import BeautifulSoup
import datetime
import pytz
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "Bild"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2006, 1, 5)
    date_end = datetime.datetime(2020, 12, 28)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield f"https://www.bild.de/archive/{date.year}/{date.month}/{date.day}/index.html"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=False)
def scrape_articles(resp):
    published_date = tuple(map(int, [re.search(r"archive/(.+?)/", resp.url).group(1),
                               re.search(r"archive/[0-9]{4}/(.+?)/", resp.url).group(1),
                               re.search(r"archive/[0-9]{4}/[0-9]{1,2}/(.+?)/", resp.url).group(1)]))

    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.select("article .txt > h2.crossheading")[0].next_siblings:
            try:
                url = "https://www.bild.de" + el.a.attrs.get("href")
                title = el.a.string
                published_str = el.contents[0]
                published_hour = int(re.search(r"^(.+?):", published_str).group(1))
                published_minute = int(re.search(r"[0-9][0-9]:(.+?) ", published_str).group(1))
                published_datetime = datetime.datetime(*published_date, published_hour, published_minute)
                tz = pytz.timezone("Europe/Berlin")
                published_datetime = tz.localize(published_datetime).astimezone(pytz.utc)

                yield url, PUBLISHER, title, published_datetime, None, None

            except:
                pass
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
