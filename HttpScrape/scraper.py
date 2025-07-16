import logging
import re
import time
import random
from datetime import datetime
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

# --- Corrected relative imports ---
from .models import JobListing
from .database import insert_job_listing
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board with pagination, concurrency, and polite request pacing."""

    def __init__(self, max_workers: int = 10, min_delay: float = 1.0, max_delay: float = 3.0):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa"
        )
        self.max_workers = max_workers
        self.min_delay = min_delay
        self.max_delay = max_delay
        # Use a session with a realistic User-Agent
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/115.0 Safari/537.36'
            )
        })

    def _throttled_get(self, url: str) -> Optional[str]:
        """Fetch a URL with a random delay to reduce bot detection."""
        time.sleep(random.uniform(self.min_delay, self.max_delay))
        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            self.logger.warning(f"Request failed for {url}: {e}")
            return None

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        # unchanged parsing logic...
        soup = BeautifulSoup(html, 'html.parser')
        # Title
        title_elem = soup.select_one('h1[data-test="text-offerTitle"]')
        title = title_elem.get_text(strip=True) if title_elem else "N/A"
        # Company
        company_elem = soup.select_one('a[data-test="anchor-company-link"]')
        company = "N/A"
        if company_elem:
            full_text = company_elem.get_text(strip=True)
            company = full_text.split(':')[-1].strip() if ':' in full_text else full_text
        # Operating mode
        mode_elem = soup.select_one('span[data-test="content-workModes"]')
        operating_mode = mode_elem.get_text(strip=True) if mode_elem else "N/A"
        # Location
        location_elem = soup.select_one('span[data-test="text-primaryLocation"]')
        location = location_elem.get_text(strip=True) if location_elem else "N/A"
        # Work type
        contract_elem = soup.select_one('span[data-test="text-contractName"]')
        work_type = "N/A"
        if contract_elem:
            match = re.search(r"\(([^)]+)\)", contract_elem.get_text())
            work_type = match.group(1) if match else contract_elem.get_text(strip=True)
        employment_type = work_type
        # Experience level
        exp_elem = soup.select_one('span[data-test="content-positionLevels"]')
        experience_level = (
            exp_elem.get_text(separator=", ", strip=True).replace('•', ',')
            if exp_elem else "N/A"
        )
        # Salary
        salary_min = salary_max = None
        salary_elem = soup.select_one('span[data-test="text-contractSalary"]')
        if salary_elem:
            nums = re.findall(r"\d+", salary_elem.get_text())
            if len(nums) >= 2:
                salary_min, salary_max = int(nums[0]), int(nums[1])
            elif nums:
                salary_min = int(nums[0])
        # Job ID
        id_elem = soup.select_one('span[data-test="text-offerId"]')
        if id_elem and id_elem.get_text(strip=True).isdigit():
            job_id = id_elem.get_text(strip=True)
        else:
            uuid_match = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
            job_id = uuid_match.group(1) if uuid_match else job_url

        return JobListing(
            job_id=job_id,
            source='theprotocol.it',
            title=title,
            company=company,
            link=job_url,
            operating_mode=operating_mode,
            salary_min=salary_min,
            salary_max=salary_max,
            location=location,
            work_type=work_type,
            experience_level=experience_level,
            employment_type=employment_type,
            years_of_experience=None,
            scrape_date=datetime.utcnow(),
            listing_status='Active'
        )

    def scrape(self) -> List[JobListing]:
        """Fetch listings across pages and parse details with throttling."""
        self.logger.info("Starting scrape with polite pacing.")
        all_jobs: List[JobListing] = []
        page = 1
        while True:
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching listing page {page}.")
            html = self._throttled_get(page_url)
            if not html:
                self.logger.info(f"Failed or no HTML for page {page}, stopping.")
                break
            soup = BeautifulSoup(html, 'html.parser')
            hrefs = [a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
            unique_hrefs = list(dict.fromkeys(hrefs))
            if not unique_hrefs:
                self.logger.info(f"No job links on page {page}, ending.")
                break
            self.logger.info(f"Page {page}: found {len(unique_hrefs)} listings.")
            # fetch details with ThreadPool but still throttling inside
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {executor.submit(self._throttled_get, self.base_url + href): href for href in unique_hrefs}
                for future in as_completed(futures):
                    href = futures[future]
                    detail_html = future.result(timeout=self.max_delay + 5)
                    if detail_html:
                        job = self._parse_job_detail(detail_html, self.base_url + href)
                        if job:
                            all_jobs.append(job)
            page += 1
        self.logger.info(f"Scraping done: total {len(all_jobs)} jobs.")
        return all_jobs


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper(max_workers=10, min_delay=1.0, max_delay=3.0)
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Inserted {len(jobs)} jobs.")
    return jobs
