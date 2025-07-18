import logging
import re
from datetime import datetime
from typing import List, Optional
import math
import requests
from bs4 import BeautifulSoup

# --- Corrected relative imports ---
from .models import JobListing
from .database import insert_job_listing
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board with dynamic Warsaw-only pagination."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        # Warsaw-only filter URL
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa;wp"
        )
        self.page_size = 50  # listings per page

    def _get_total_pages(self, html: str) -> int:
        """Parses the total number of job offers to determine the number of pages."""
        soup = BeautifulSoup(html, 'html.parser')
        # This selector might need to be adjusted if the website structure changes.
        total_offers_text = soup.select_one('h2.results-header__title > span')
        if total_offers_text and total_offers_text.text.isdigit():
            total_offers = int(total_offers_text.text)
            return math.ceil(total_offers / self.page_size)
        return 1 # Default to 1 page if the total can't be determined

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title_elem = soup.select_one('h1[data-test="text-offerTitle"]')
            title = title_elem.get_text(strip=True) if title_elem else "N/A"
            company_elem = soup.select_one('a[data-test="anchor-company-link"]')
            company = company_elem.get_text(strip=True).split(':')[-1].strip() if company_elem else "N/A"
            mode_elem = soup.select_one('span[data-test="content-workModes"]')
            operating_mode = mode_elem.get_text(strip=True) if mode_elem else "N/A"
            loc_elem = soup.select_one('span[data-test="text-primaryLocation"]')
            location = loc_elem.get_text(strip=True) if loc_elem else "N/A"
            contract_elem = soup.select_one('span[data-test="text-contractName"]')
            contract_text = contract_elem.get_text(strip=True) if contract_elem else ""
            m = re.search(r"\(([^)]+)\)", contract_text)
            work_type = m.group(1) if m else contract_text or "N/A"
            exp_elem = soup.select_one('span[data-test="content-positionLevels"]')
            experience = exp_elem.get_text(separator=", ", strip=True).replace('•', ',') if exp_elem else "N/A"
            salary_elem = soup.select_one('span[data-test="text-contractSalary"]')
            nums = re.findall(r"\d+", salary_elem.get_text().replace(" ", "")) if salary_elem else []
            if len(nums) >= 2:
                salary_min, salary_max = int(nums[0]), int(nums[1])
            elif nums:
                salary_min, salary_max = int(nums[0]), None
            else:
                salary_min = salary_max = None
            id_elem = soup.select_one('span[data-test="text-offerId"]')
            if id_elem and id_elem.get_text(strip=True).isdigit():
                job_id = id_elem.get_text(strip=True)
            else:
                uuid_m = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
                job_id = uuid_m.group(1) if uuid_m else job_url

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
                experience_level=experience,
                employment_type=work_type,
                years_of_experience=None,
                scrape_date=datetime.utcnow(),
                listing_status='Active'
            )
        except Exception as e:
            self.logger.error(f"Error parsing detail {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[JobListing]:
        """Paginates Warsaw-only listings, stopping when the last page is reached."""
        self.logger.info("Starting Warsaw-only pagination scrape.")
        all_jobs: List[JobListing] = []

        # First, get the total number of pages
        first_page_html = self.get_page_html(self.search_url)
        if not first_page_html:
            self.logger.error("Could not fetch the first page. Aborting scrape.")
            return []
        
        total_pages = self._get_total_pages(first_page_html)
        self.logger.info(f"Total pages to scrape: {total_pages}")

        for page in range(1, total_pages + 1):
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching page {page}/{total_pages}: {page_url}")
            html = self.get_page_html(page_url)
            if not html:
                self.logger.warning(f"No HTML for page {page}, stopping.")
                break

            soup = BeautifulSoup(html, 'html.parser')
            # The selector for the links to job offers
            hrefs = [a['href'] for a in soup.select('a[href*=",oferta,"]')]
            unique_hrefs = list(dict.fromkeys(hrefs))
            self.logger.info(f"Page {page}: Found {len(unique_hrefs)} unique listings.")

            if not unique_hrefs:
                self.logger.info(f"No listings found on page {page}. Ending scrape.")
                break

            for href in unique_hrefs:
                job_url = self.base_url + href
                detail_html = self.get_page_html(job_url)
                if detail_html:
                    job = self._parse_job_detail(detail_html, job_url)
                    if job:
                        all_jobs.append(job)

        self.logger.info(f"Scrape complete: {len(all_jobs)} jobs.")
        return all_jobs


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Inserted {len(jobs)} jobs.")
    return jobs
