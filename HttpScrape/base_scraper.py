import logging
import requests
import time
import random
from abc import ABC, abstractmethod
from typing import List

class BaseScraper(ABC):
    """Base class for all job scrapers"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,pl;q=0.8',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        }
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_page_html(self, url: str, max_retries=3, base_delay=1.0) -> str:
        """Get HTML content from a URL with retry logic."""
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(base_delay, base_delay + 1.5))
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed for {url} (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep((2 ** attempt) * base_delay) # Exponential backoff
        
        self.logger.error(f"Failed to fetch {url} after {max_retries} attempts.")
        return ""

    @abstractmethod
    def scrape(self) -> List:
        """Main scraping method to be implemented by each specific scraper."""
        pass
