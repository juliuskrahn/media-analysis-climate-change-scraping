from bs4 import BeautifulSoup
import datetime
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
from crawler import Crawler
from utils import db_connection_cursor
from psycopg2.extras import execute_values
import logging


PUBLISHER = "FoxNews"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    def load_from_cache():
        # -> db as a cache
        with db_connection_cursor() as cur_r:
            cur_r.execute("SELECT key FROM temp WHERE publisher = %s", [PUBLISHER])
            for record in cur_r:
                yield record[0]

    urls_from_cache = [*load_from_cache()]
    if len(urls_from_cache) > 0:
        return urls_from_cache

    def get_urls_and_then_cache(response_chunk):
        urls = []
        soup = BeautifulSoup(response_chunk[0].text, "xml")
        for el in soup.find_all("loc"):
            urls.append((PUBLISHER, el.string))
        with db_connection_cursor() as cur_w:
            execute_values(cur_w,
                           "INSERT INTO temp (PUBLISHER, key) "
                           "VALUES %s ON CONFLICT DO NOTHING",
                           urls)

    crawler = Crawler(["https://www.foxnews.com/sitemap.xml"], get_urls_and_then_cache)
    crawler.run()

    return load_from_cache()


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "xml")
    try:
        for el in soup.find_all("url"):
            url = el.loc.string
            published = datetime.datetime.fromisoformat(el.lastmod.string)
            yield url, PUBLISHER, None, published, None, None

    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")
