import logging
import re
from datetime import datetime
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

# --- Corrected relative imports ---
from .models import JobListing
from .database import insert_job_listing
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board with pagination and unified numeric job IDs."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa;wp"
        )

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        """Parses the HTML of a single job listing page and extracts fields."""
        try:
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
            # Work type and Employment type (extract bracket content)
            work_type = "N/A"
            contract_elem = soup.select_one('span[data-test="text-contractName"]')
            if contract_elem:
                bracket_match = re.search(r"\(([^)]+)\)", contract_elem.get_text())
                work_type = bracket_match.group(1) if bracket_match else contract_elem.get_text(strip=True)
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
                elif len(nums) == 1:
                    salary_min = int(nums[0])

            # Job ID: numeric ID if available, else UUID
            job_id = None
            id_elem = soup.select_one('span[data-test="text-offerId"]')
            if id_elem and id_elem.get_text(strip=True).isdigit():
                job_id = id_elem.get_text(strip=True)
            else:
                uuid_match = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
                job_id = uuid_match.group(1) if uuid_match else job_url

            job = JobListing(
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
            self.logger.info(f"Parsed job {job_id}: {title}")
            return job
        except Exception as e:
            self.logger.error(f"Error parsing detail {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[JobListing]:
        """Main scraping method: iterates through pagination until no more listings."""
        self.logger.info("Starting pagination scrape.")
        all_jobs: List[JobListing] = []
        page = 1
        while True:
            url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching page {page}: {url}")
            html = self.get_page_html(url)
            if not html:
                self.logger.info(f"No HTML for page {page}, stopping.")
                break
            soup = BeautifulSoup(html, 'html.parser')
            links = [a for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
            if not links:
                self.logger.info(f"No listings on page {page}, done.")
                break
            # Deduplicate by href
            seen = set()
            job_links = []
            for a in links:
                href = a['href']
                if href not in seen:
                    seen.add(href)
                    job_links.append(a)
            self.logger.info(f"Found {len(job_links)} job links on page {page}.")

            for link_elem in job_links:
                job_url = self.base_url + link_elem['href']
                detail_html = self.get_page_html(job_url)
                if detail_html:
                    job = self._parse_job_detail(detail_html, job_url)
                    if job:
                        all_jobs.append(job)
            page += 1
        self.logger.info(f"Scrape complete: {len(all_jobs)} jobs total.")
        return all_jobs


def run_scraper():
    """Runs scraper and inserts into DB."""
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Finished: {len(jobs)} jobs inserted.")
    return jobs
