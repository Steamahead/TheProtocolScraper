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
    """Scraper for theprotocol.it job board."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa"
        )

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        """Parses the HTML of a single job listing page."""
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
            # Work type
            work_type_elem = soup.select_one('span[data-test="text-contractName"]')
            work_type = work_type_elem.get_text(strip=True) if work_type_elem else "N/A"
            # Experience level
            exp_elem = soup.select_one('span[data-test="content-positionLevels"]')
            experience_level = (
                exp_elem.get_text(separator=", ", strip=True)
                .replace('•', ',') if exp_elem else "N/A"
            )
            # Employment type (same selector as work type)
            employment_type = work_type
            # Salary
            salary_elem = soup.select_one('span[data-test="text-contractSalary"]')
            salary_min = salary_max = None
            if salary_elem:
                # Extract numbers
                nums = re.findall(r"\d+", salary_elem.get_text())
                if len(nums) >= 2:
                    salary_min, salary_max = int(nums[0]), int(nums[1])
                elif len(nums) == 1:
                    salary_min = int(nums[0])

            # Job ID extraction (use the UUID after ',oferta,')
            job_id_match = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
            job_id = job_id_match.group(1) if job_id_match else job_url

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
            self.logger.info(f"Successfully parsed job: {job.title} at {job.company}")
            return job
        except Exception as e:
            self.logger.error(f"Error parsing detail page {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[JobListing]:
        """Main scraping method for The Protocol."""
        self.logger.info(f"Fetching listings from {self.search_url}")
        listings_html = self.get_page_html(self.search_url)
        # INFO: dump first 500 chars so we can inspect the page structure in logs
        self.logger.info("Listings HTML snippet:\n" + (listings_html or '')[:500])

        if not listings_html:
            self.logger.error("Failed to fetch the main listings page.")
            return []

        soup = BeautifulSoup(listings_html, 'html.parser')
        # Try the data-test selector first
        job_links = soup.select('a[data-test="link-offer"]')
        if not job_links:
            self.logger.warning(
                "Primary selector returned 0 links; dumping all <a href> containing 'oferta'."
            )
            candidate_links = [a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
            self.logger.info(f"Found {len(candidate_links)} hrefs with ',oferta,': {candidate_links}")
            # Fallback: pick links containing ',oferta,'
            job_links = [
                a for a in soup.find_all('a', href=True)
                if ',oferta,' in a['href']
            ]
            self.logger.info(f"After fallback filter: {len(job_links)} links")

        all_jobs: List[JobListing] = []
        for link_elem in job_links:
            job_url = self.base_url + link_elem['href']
            detail_html = self.get_page_html(job_url)
            if detail_html:
                job_listing = self._parse_job_detail(detail_html, job_url)
                if job_listing:
                    all_jobs.append(job_listing)

        self.logger.info(f"Scrape complete. Found {len(all_jobs)} total jobs.")
        return all_jobs


def run_scraper():
    """Initializes and runs the scraper."""
    logging.info("Scraper process started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Scraping finished. {len(jobs)} jobs processed.")
    return jobs
