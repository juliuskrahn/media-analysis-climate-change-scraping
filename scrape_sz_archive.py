from bs4 import BeautifulSoup
import datetime
import pytz
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
from crawler import Crawler
from utils import db_connection_cursor
from psycopg2.extras import execute_values
import re
import logging

PUBLISHER = "SZ"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    def generate_from_cache():
        # -> db as a cache
        cached = False
        with db_connection_cursor() as cur_r:
            cur_r.execute("SELECT key, val FROM temp WHERE publisher = %s", [PUBLISHER])
            for record in cur_r:
                cached = True
                if int(record[1]) == 1:
                    yield record[0]
                    continue
                for num in range(1, int(record[1]) + 1):
                    yield f"{record[0]}/page/{num}"
        if not cached:
            raise LookupError

    try:
        urls = [*generate_from_cache()]
        return urls
    except LookupError:
        pass

    # generate from scratch
    # (and then cache, generate from cache)

    categories = [
        "politik",
        "wirtschaft",
        "geld",
        "panorama",
        "sport",
        "münchen",
        "bayern",
        "landkreis",
        "region",
        "kultur",
        "medien",
        "wissen",
        "gesundheit",
        "digital",
        "karriere",
        "bildung",
        "reise",
        "auto",
        "stil",
        "leben"
    ]

    root_urls = []

    for category in categories:
        for year in range(2015, 2021):
            for month in range(1, 13):
                month_datetime = datetime.datetime(year, month, 1)
                root_urls.append(f"https://www.sueddeutsche.de/archiv/{category}/{month_datetime.strftime('%Y/%m')}")

    def get_page_num_for_category_months_and_then_cache(response_chunk):
        urls_and_last_page_nums = []
        for resp in response_chunk:
            try:
                soup = BeautifulSoup(resp.text, "lxml")
                navigation_el = soup.find("li", class_="navigation")
                last_page_num = navigation_el.find_all("a")[-1].string
                urls_and_last_page_nums.append((PUBLISHER, resp.url, last_page_num))
            except IndexError:
                urls_and_last_page_nums.append((PUBLISHER, resp.url, 1))

        with db_connection_cursor() as cur_w:
            execute_values(cur_w,
                           "INSERT INTO temp (PUBLISHER, key, val) "
                           "VALUES %s ON CONFLICT DO NOTHING",
                           urls_and_last_page_nums)

    crawler = Crawler(root_urls, get_page_num_for_category_months_and_then_cache, response_chunk_size=32)
    crawler.run()

    return generate_from_cache()


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "lxml")
    try:
        entrylist = soup.find(id="entrylist-container")
        for entry in entrylist.find_all(class_="entrylist__entry"):
            url = entry.find(class_="entrylist__link").attrs.get("href")
            try:
                published_str = entry.find("time").string.strip()
                published_str_re_result = re.search(r"^(.+)\.(.+)\.(.+) \| (.+):(.+)$", published_str)
                published = datetime.datetime(int(published_str_re_result.group(3)),
                                              int(published_str_re_result.group(2)),
                                              int(published_str_re_result.group(1)),
                                              int(published_str_re_result.group(4)),
                                              int(published_str_re_result.group(5)))
                tz = pytz.timezone("Europe/Berlin")
                published = tz.localize(published).astimezone(pytz.utc)
            except AttributeError:
                archive_url_re_result = re.search(r"/([0-9]{4})/([0-9]{2})", resp.url)
                published = datetime.datetime(int(archive_url_re_result.group(1)),
                                              int(archive_url_re_result.group(2)),
                                              1,
                                              0,
                                              0)
            title = entry.find(class_="entrylist__title").string

            breadcrumbs = [el.string.strip() for el in entry.find_all(class_="breadcrumb-list__item")]
            overline = entry.find(class_="entrylist__overline")
            if overline:
                overline = overline.string
            tags = [overline, breadcrumbs[0]]
            category = None
            if len(breadcrumbs) > 1:
                category = "; ".join(breadcrumbs[1:])

            yield url, PUBLISHER, title, published, category, tags

    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
