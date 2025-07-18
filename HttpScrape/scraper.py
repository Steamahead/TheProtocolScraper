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
    """Scraper for theprotocol.it job board with dynamic Warsaw-only pagination based on total count."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        # Warsaw-only filter URL
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa;wp"
        )
        self.page_size = 50  # listings per page

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title_elem = soup.select_one('h1[data-test="text-offerTitle"]')
            title = title_elem.get_text(strip=True) if title_elem else "N/A"
            company_elem = soup.select_one('a[data-test="anchor-company-link"]')
            company = company_elem.get_text(strip=True).split(':')[-1].strip() if company_elem else "N/A"
            mode = soup.select_one('span[data-test="content-workModes"]')
            operating_mode = mode.get_text(strip=True) if mode else "N/A"
            loc = soup.select_one('span[data-test="text-primaryLocation"]')
            location = loc.get_text(strip=True) if loc else "N/A"
            contract_elem = soup.select_one('span[data-test="text-contractName"]')
            contract_text = contract_elem.get_text(strip=True) if contract_elem else ""
            m = re.search(r"\(([^)]+)\)", contract_text)
            work_type = m.group(1) if m else contract_text or "N/A"
            experience_elem = soup.select_one('span[data-test="content-positionLevels"]')
            experience = experience_elem.get_text(separator=", ", strip=True).replace('•', ',') if experience_elem else "N/A"
            salary_elem = soup.select_one('span[data-test="text-contractSalary"]')
            nums = re.findall(r"\d+", salary_elem.get_text()) if salary_elem else []
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
        """Scrapes all Warsaw-only pages based on total count from page 1, with debug fallback for total."""
        self.logger.info("Starting Warsaw-only scrape based on total count.")
        all_jobs: List[JobListing] = []

        # Fetch page 1 to get total count
        first_page_url = f"{self.search_url}?page=1"
        first_html = self.get_page_html(first_page_url)
        if not first_html:
            self.logger.error("Failed to fetch first page.")
            return all_jobs
        # Parse total offers: try DOM first, then regex
        soup_first = BeautifulSoup(first_html, 'html.parser')
        text_node = soup_first.find(string=re.compile(r"Wyniki \(\d+"))
        if text_node:
            total = int(re.search(r"(\d+)", text_node).group(1))
            self.logger.info(f"Total offers parsed from page text: {total}")
        else:
            self.logger.debug("First-page HTML snippet for debugging total count:\n" + first_html[:500])
            self.logger.warning("Could not find total via DOM; falling back to regex.")
            match = re.search(r"Wyniki \((\d+)\)", first_html)
            total = int(match.group(1)) if match else None
        if total:
            pages = math.ceil(total / self.page_size)
            self.logger.info(f"Total offers: {total}, pages: {pages}")
        else:
            self.logger.warning("Could not determine total; defaulting to single page.")
            pages = 1

        # Iterate pages
        for page in range(1, pages + 1):
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching page {page}: {page_url}")
            html = self.get_page_html(page_url)
            if not html:
                self.logger.warning(f"No HTML for page {page}, stopping.")
                break
            soup = BeautifulSoup(html, 'html.parser')
            hrefs = [a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
            unique = list(dict.fromkeys(hrefs))
            self.logger.info(f"Page {page}: {len(unique)} listings.")
            for href in unique:
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
