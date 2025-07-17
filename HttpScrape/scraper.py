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
    """Scraper for theprotocol.it job board with precise Warsaw-only pagination."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        # Warsaw-only filter URL
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa;wp"
        )
        # Number of listings per page as seen on site
        self.page_size = 50

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        """Parses detail page HTML and returns a JobListing."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Extract fields as before...
            title_elem = soup.select_one('h1[data-test="text-offerTitle"]')
            title = title_elem.get_text(strip=True) if title_elem else "N/A"
            company_elem = soup.select_one('a[data-test="anchor-company-link"]')
            company = company_elem.get_text(strip=True).split(':')[-1].strip() if company_elem else "N/A"
            operating_mode = soup.select_one('span[data-test="content-workModes"]').get_text(strip=True) if soup.select_one('span[data-test="content-workModes"]') else "N/A"
            location = soup.select_one('span[data-test="text-primaryLocation"]').get_text(strip=True) if soup.select_one('span[data-test="text-primaryLocation"]') else "N/A"
            contract_text = soup.select_one('span[data-test="text-contractName"]').get_text(strip=True) if soup.select_one('span[data-test="text-contractName"]') else ""
            work_type_match = re.search(r"\(([^)]+)\)", contract_text)
            work_type = work_type_match.group(1) if work_type_match else contract_text or "N/A"
            experience = soup.select_one('span[data-test="content-positionLevels"]').get_text(separator=", ", strip=True).replace('•', ',') if soup.select_one('span[data-test="content-positionLevels"]') else "N/A"
            salary_nums = re.findall(r"\d+", soup.select_one('span[data-test="text-contractSalary"]').get_text()) if soup.select_one('span[data-test="text-contractSalary"]') else []
            salary_min, salary_max = (int(salary_nums[0]), int(salary_nums[1])) if len(salary_nums) >= 2 else (int(salary_nums[0]), None) if salary_nums else (None, None)
            id_elem = soup.select_one('span[data-test="text-offerId"]')
            if id_elem and id_elem.get_text(strip=True).isdigit():
                job_id = id_elem.get_text(strip=True)
            else:
                match = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
                job_id = match.group(1) if match else job_url

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
        """Paginates through Warsaw-only listings until fewer than page_size results."""
        self.logger.info("Starting Warsaw-only pagination scrape.")
        all_jobs: List[JobListing] = []
        page = 1
        while True:
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching page {page}: {page_url}")
            html = self.get_page_html(page_url)
            if not html:
                self.logger.info("No HTML returned; stopping.")
                break
            soup = BeautifulSoup(html, 'html.parser')
            hrefs = [a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
            # dedupe
            unique = list(dict.fromkeys(hrefs))
            count = len(unique)
            self.logger.info(f"Page {page}: found {count} Warsaw listings.")
            if count == 0:
                break
            for href in unique:
                job_url = self.base_url + href
                detail_html = self.get_page_html(job_url)
                if detail_html:
                    job = self._parse_job_detail(detail_html, job_url)
                    if job:
                        all_jobs.append(job)
            # stop when last page (fewer than page_size)
            if count < self.page_size:
                self.logger.info(f"Last Warsaw page {page} has {count} listings; ending.")
                break
            page += 1
        self.logger.info(f"Scrape complete: {len(all_jobs)} total jobs.")
        return all_jobs


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Inserted {len(jobs)} jobs.")
    return jobs
