import psycopg2
from psycopg2.extras import execute_values
from contextlib import contextmanager
import logging


@contextmanager
def db_connection_cursor():
    conn = psycopg2.connect(dbname="media_analysis", user="julius", password="", host="localhost", port=5432)
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except:
        logging.exception("DB Transaction Error")
    finally:
        conn.close()


def unprocessed_archive_urls(publisher):
    """ Filter out the already processed urls """
    def decorator(archive_urls_func):
        def wrapper():
            processed_urls = []
            with db_connection_cursor() as cur:
                cur.execute("SELECT url FROM processed_archive_page WHERE publisher = %s", [publisher])
                for record in cur:
                    processed_urls.append(record[0])

            logging.debug(f"Archive URLs that are processed: {processed_urls}")

            unprocessed_urls = []
            for url in archive_urls_func():
                if url not in processed_urls:
                    unprocessed_urls.append(url)

            logging.debug(f"Archive URLs to process: {unprocessed_urls}")

            return unprocessed_urls

        return wrapper
    return decorator


def process_crawled_archive_response_chunk(publisher, write_to_db=False):
    """ Run the decorated function over each response and write the returned (scraped) data to the db
    (- if write_to_db)
    Also write the processed urls to the db, so that one can stop and rerun the script at any time
    """

    def decorator(scrape_articles_func):
        def wrapper(resp_chunk):
            all_articles = []
            processed_archive_pages = []
            for resp in resp_chunk:
                all_articles.extend(scrape_articles_func(resp))
                processed_archive_pages.append((publisher, resp.url))

            if write_to_db:
                if logging.DEBUG == logging.root.level:
                    logging.warning(f"Writing to DB with root logging level DEBUG!")

                with db_connection_cursor() as cur:
                    execute_values(cur,
                                   "INSERT INTO article (url, publisher, title, author, published, category, tags) "
                                   "VALUES %s ON CONFLICT DO NOTHING",
                                   all_articles)
                    execute_values(cur,
                                   "INSERT INTO processed_archive_page (publisher, url) "
                                   "VALUES %s ON CONFLICT DO NOTHING",
                                   processed_archive_pages)

            logging.info(f"Processed archive pages: {processed_archive_pages}")
            logging.debug(f"Processed articles: {all_articles}")

        return wrapper
    return decorator
