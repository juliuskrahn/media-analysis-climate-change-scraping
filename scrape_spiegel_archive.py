from bs4 import BeautifulSoup
import datetime
import pytz
import re
from utils import unprocessed_archive_urls, process_crawled_archive_response_chunk
import logging


PUBLISHER = "Spiegel"


@unprocessed_archive_urls(PUBLISHER)
def archive_urls():
    date_start = datetime.datetime(2000, 1, 1)
    date_end = datetime.datetime(2020, 12, 28)
    for day in range((date_end - date_start).days + 1):
        date = date_start + datetime.timedelta(days=day)
        yield f"https://www.spiegel.de/nachrichtenarchiv/artikel-{date.strftime('%d.%m.%Y')}.html"


@process_crawled_archive_response_chunk(PUBLISHER, write_to_db=False)
def scrape_articles(resp):
    def filter_texts(texts):
        return list(filter(lambda text: text not in ["\n", "", " "], texts))

    published_date = tuple(map(int, [re.search(r"artikel-[0-9][0-9]\.[0-9][0-9]\.(.+?)\.", resp.url).group(1),
                               re.search(r"artikel-[0-9][0-9]\.(.+?)\.", resp.url).group(1),
                               re.search(r"artikel-(.+?)\.", resp.url).group(1)]))

    soup = BeautifulSoup(resp.text, "lxml")
    for el in soup.find_all("article"):
        try:
            url = el.header.a.attrs.get("href")

            title = el["aria-label"]

            footer_texts = filter_texts(el.footer.find_all(text=True))
            published_str = footer_texts[0].string
            published_hour = int(re.search(r", (.+?)\.", published_str).group(1))
            published_minute = int(re.search(r"[0-9].*\.(.+?) Uhr", published_str).group(1))
            published = datetime.datetime(*published_date, published_hour, published_minute)
            tz = pytz.timezone("Europe/Berlin")
            published = tz.localize(published).astimezone(pytz.utc)
            category = footer_texts[2].string

            tags = []
            potential_tags = el.find_all("title")
            if potential_tags:
                for tag_el in potential_tags:
                    tags.append(tag_el.string)

            yield url, PUBLISHER, title, published, category, tags

        except:
            logging.exception(f"An Error occurred while trying to retrieve an article from the archive: {resp.url}")
