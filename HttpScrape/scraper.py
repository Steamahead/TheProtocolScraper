import logging
import re
from datetime import datetime
from typing import List, Optional
import requests
from bs4 import BeautifulSoup

# --- CORRECTED RELATIVE IMPORTS ---
from .models import JobListing
from .database import insert_job_listing
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = "https://theprotocol.it/filtry/big-data-science;sp/junior,assistant,trainee,mid;p/warszawa"

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        """Parses the HTML of a single job listing page."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title_elem = soup.select_one('h1[data-test="text-offerTitle"]')
            title = title_elem.get_text(strip=True) if title_elem else "N/A"
            company_elem = soup.select_one('a[data-test="anchor-company-link"]')
            company = "N/A"
            if company_elem:
                full_text = company_elem.get_text(strip=True)
                if ':' in full_text:
                    company = full_text.split(':')[-1].strip()
                else:
                    company = full_text
            mode_elem = soup.select_one('span[data-test="content-workModes"]')
            operating_mode = mode_elem.get_text(strip=True) if mode_elem else "N/A"
            job_id_match = re.search(r'oferta,([a-zA-Z0-9\-]+)$', job_url)
            job_id = job_id_match.group(1) if job_id_match else job_url

            job = JobListing(
                job_id=job_id, source='theprotocol.it', title=title, company=company,
                link=job_url, operating_mode=operating_mode, salary_min=None, salary_max=None,
                location="N/A", work_type="N/A", experience_level="N/A", employment_type="N/A",
                years_of_experience=None, scrape_date=datetime.utcnow(), listing_status='Active'
            )
            logging.info(f"Successfully parsed job: {job.title} at {job.company}")
            return job
        except Exception as e:
            self.logger.error(f"Error parsing detail page {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[JobListing]:
        """Main scraping method for The Protocol."""
        self.logger.info(f"Fetching listings from {self.search_url}")
        listings_html = self.get_page_html(self.search_url)
        # — DEBUG: dump first 500 chars so we can inspect the page structure in logs
        self.logger.debug("Listings HTML snippet:\n" + listings_html[:500])

        if not listings_html:
            self.logger.error("Failed to fetch the main listings page.")
            return []

        soup = BeautifulSoup(listings_html, 'html.parser')
        # try the data-test selector first
        job_links = soup.select('a[data-test="link-offer"]')
        if not job_links:
            self.logger.warning(
                "Primary selector returned 0 links; dumping all <a href> containing 'oferta'."
            )
            candidate_links = [a['href'] for a in soup.find_all('a', href=True) if 'oferta' in a['href']]
            self.logger.debug(f"Found {len(candidate_links)} hrefs with 'oferta': {candidate_links}")
            # now pick whichever pattern matches your site (comma? slash? both?)
            job_links = [
                a for a in soup.find_all('a', href=True)
                if a['href'].startswith('/oferta')  # simpler: any link starting with "/oferta"
            ]
            self.logger.info(f"After startswith('/oferta') filter: {len(job_links)} links")
        
        all_jobs = []
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
