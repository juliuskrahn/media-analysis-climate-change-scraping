#!.venv/bin/python
import logging
from crawler import Crawler
from scrape_bild_archive import archive_urls, scrape_articles
# import funcs from desired scraping file (and make sure to set write_to_db to True/False)


logging.basicConfig(filename='.log',
                    format="[%(asctime)s]%(levelname)s:%(message)s",
                    level=logging.INFO)
crawler = Crawler(archive_urls(), scrape_articles, response_chunk_size=32)
crawler.run()
