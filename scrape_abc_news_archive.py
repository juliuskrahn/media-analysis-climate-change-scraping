from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk, db_connection_cursor
import logging
from psycopg2.extras import execute_values


PUBLISHER = "ABCNews"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2015, 1, 1)
    date_end = datetime.datetime(2020, 12, 31)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield "https://www.abc.net.au/news/archive/?date=" + date.strftime("%Y-%m-%d")
    # load cached pagination pages
    with db_connection_cursor() as cur:
        cur.execute("SELECT key FROM temp WHERE publisher = %s", [PUBLISHER])
        for record in cur:
            yield record[0]


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    resp_url_re_result = re.search(r"=([0-9]{4})-([0-9]{2})-([0-9]{2})", resp.url)
    published = datetime.datetime(int(resp_url_re_result.group(1)),
                                  int(resp_url_re_result.group(2)),
                                  int(resp_url_re_result.group(3)))

    soup = BeautifulSoup(resp.text, "lxml")
    try:
        for el in soup.find_all("article", class_="view-teaser-richWide"):
            try:
                header = el.find("header").find("a")
                url = "https://www.abc.net.au/news/archive" + header.attrs.get("href")
                title = header.string
                yield url, PUBLISHER, title, published, None, None
            except AttributeError:
                continue
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")


def pagination_handler(resp):
    soup = BeautifulSoup(resp.text, "lxml")
    next_el = soup.find("a", class_="next icon-")
    if not next_el:
        return
    url = "https://www.abc.net.au/news/archive/" + next_el.attrs["href"]
    with db_connection_cursor() as cur:  # cache
        execute_values(cur,
                       "INSERT INTO temp (PUBLISHER, key) "
                       "VALUES %s ON CONFLICT DO NOTHING",
                       [(PUBLISHER, url)])
    return url
