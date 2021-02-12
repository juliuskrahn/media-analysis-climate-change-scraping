from bs4 import BeautifulSoup
import datetime
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk, db_connection_cursor
import logging
from psycopg2.extras import execute_values


PUBLISHER = "Emol"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    for year in range(2015, 2021):
        for month in range(1, 13):
            yield f"https://www.emol.com/sitemap/noticias/{year}/emol_noticias_{year}_{month:0>2}_0000001.html"
    # load cached pagination pages
    with db_connection_cursor() as cur:
        cur.execute("SELECT key FROM temp WHERE publisher = %s", [PUBLISHER])
        for record in cur:
            yield record[0]


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=True)
def scrape_articles(resp):
    soup = BeautifulSoup(resp.text, "lxml")
    resp_url_re_result = re.search(r"emol_noticias_([0-9]{4})_([0-9]{2})", resp.url)
    archive_page_month_datetime = datetime.datetime(int(resp_url_re_result.group(1)),
                                                    int(resp_url_re_result.group(2)),
                                                    1)
    try:
        for el in soup.select("#mainContent ul li a"):
            url = el.attrs.get("href")
            try:
                url_re_result = re.search(r"/([0-9]{4})/([0-9]{2})/([0-9]{2})/", url)
                published = datetime.datetime(int(url_re_result.group(1)),
                                              int(url_re_result.group(2)),
                                              int(url_re_result.group(3)),
                                              0,
                                              0)
            except AttributeError:
                published = archive_page_month_datetime
            yield url, PUBLISHER, el.string, published, None, None
    except:
        logging.exception(f"Failed to parse archive page: {resp.url}")


def pagination_handler(resp):
    soup = BeautifulSoup(resp.text, "lxml")
    nav_links = soup.select("#mainContent > a")
    if len(nav_links) == 2:
        url = re.search("(.*)emol_noticias_.*", resp.url).group(1) + nav_links[1].attrs.get("href")
    elif nav_links[0].string == "Siguiente >>":
        url = re.search("(.*)emol_noticias_.*", resp.url).group(1) + nav_links[0].attrs.get("href")
    else:
        return
    with db_connection_cursor() as cur:  # cache
        execute_values(cur,
                       "INSERT INTO temp (PUBLISHER, key) "
                       "VALUES %s ON CONFLICT DO NOTHING",
                       [(PUBLISHER, url)])
    return url
