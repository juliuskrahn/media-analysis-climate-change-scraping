import requests
import threading
import time
import random
from contextlib import contextmanager
import logging


class Crawler:
    """ Requests urls in chunks and then calls the processor func to process the chunk of responses...
    This is done so that the processor func does not have to worry about writing to the db too often.
    Requests are randomized a little bit - to avoid being blocked (not sure if this even helps though).
    """

    def __init__(self, urls, processor_func, response_chunk_size=32, cookies=None):
        self.urls = urls
        self.processor_func = processor_func
        self.response_chunk = []
        self.response_chunk_size = response_chunk_size
        self.cookies = cookies if cookies else {}

    def run(self):
        """ Start crawling... configured on init. """
        error_count = 0
        while len(self.urls) >= 1 and error_count < 10:
            try:
                response = self.make_request(self.urls.pop())
                self.response_chunk.append(response)
                with self.process_response_chunk():
                    self.wait_before_next_request()
                error_count = 0
            except requests.exceptions.HTTPError or requests.exceptions.Timeout as e:
                error_count += 1
                logging.exception(e)
        if error_count == 10:
            raise ConnectionError

    def make_request(self, url):
        tries = 0
        while tries < 10:
            try:
                response = requests.get(url, headers=self.request_headers(), cookies=self.cookies, timeout=5)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                tries += 1
        raise requests.exceptions.Timeout

    @contextmanager
    def process_response_chunk(self):
        if len(self.response_chunk) >= self.response_chunk_size \
                or (len(self.response_chunk) > 0 and len(self.urls) == 0):
            processor_func_thread = threading.Thread(target=self.processor_func, args=[self.response_chunk])
            processor_func_thread.start()
            yield
            processor_func_thread.join(timeout=5)
            self.response_chunk = []
        else:
            yield

    @staticmethod
    def wait_before_next_request():
        time.sleep(random.choices([
            random.randint(60, 90),
            random.randint(30, 40),
            random.randint(5, 10),
            random.randint(2, 3),
            random.randint(1, 2),
            random.randint(0, 1),
            0
        ], weights=[
            2,
            10,
            10,
            100,
            200,
            500,
            1000
        ], k=1)[0])

    @staticmethod
    def request_headers():
        return {
            "User-Agent":
                random.choice([
                    "Mozilla/5.0 (Linux; U; Android 2.2) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile "
                    "Safari/533.1",
                    "Mozilla/5.0 (Linux; Android 4.2.1; en-us; Nexus 5 Build/JOP40D) AppleWebKit/535.19 (KHTML, "
                    "like Gecko; googleweblight) Chrome/38.0.1025.166 Mobile Safari/535.19",
                    "Mozilla/5.0 (Linux; Android 6.0.1; RedMi Note 5 Build/RB3N5C; wv) AppleWebKit/537.36 (KHTML, "
                    "like Gecko) Version/4.0 Chrome/68.0.3440.91 Mobile Safari/537.36",
                    "Mozilla/5.0 (Linux; Android 7.1.2; AFTMM Build/NS6265; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Version/4.0 Chrome/70.0.3538.110 Mobile Safari/537.36",
                    "Mozilla/5.0 (Linux; Android 6.0; LG-H631 Build/MRA58K) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Version/4.0 Chrome/38.0.2125.102 Mobile Safari/537.36",
                    "Mozilla/5.0 (Linux; Android 7.1.2; AFTMM Build/NS6264; wv) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Version/4.0 Chrome/59.0.3071.125 Mobile Safari/537.36",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Mobile/15E148",
                    "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Mobile/15E148",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/12.1 Mobile/15E148 Safari/604.1",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Mobile/15E148",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/74.0.3729.169 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 "
                    "Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/64.0.3282.140 Safari/537.36 Edge/18.17763",
                    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/54.0",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                    "Version/11.1.2 Safari/605.1.15",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 "
                    "Safari/537.36 "
                ]),
            "Accept-Language":
                random.choice([
                    "de",
                    "de,en;q=0.5",
                    "de-CH,en;q=0.5",
                    "en-US,en;q=0.5"
                ]),
            "Accept-Encoding":
                random.choice([
                    "gzip, deflate, br, compress",
                    "gzip, deflate, br"
                ]),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,"
                      "*/*;q=0.8, application/signed-exchange;v=b3;q=0.9"
        }
