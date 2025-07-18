import logging
import re
from datetime import datetime
from typing import List, Optional, Set
import math
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# --- Corrected relative imports ---
from .models import JobListing
from .database import insert_job_listing
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board with dynamic Warsaw-only pagination."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa;wp"
        )
        self.page_size = 50
        self.max_pages = 10  # Safety limit

    def _extract_job_urls_from_page(self, html: str) -> Set[str]:
        """Extract unique job URLs from a page."""
        soup = BeautifulSoup(html, 'html.parser')
        hrefs = {a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']}
        return hrefs

    def _detect_actual_pages(self) -> int:
        """Detect the actual number of pages by checking for duplicate content."""
        self.logger.info("Detecting actual number of pages by checking for unique content...")
        
        seen_urls: Set[str] = set()
        page = 1
        
        while page <= self.max_pages:
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Testing page {page}: {page_url}")
            
            html = self.get_page_html(page_url)
            if not html:
                self.logger.warning(f"Page {page} returned no content")
                break
                
            current_urls = self._extract_job_urls_from_page(html)
            
            if not current_urls:
                self.logger.info(f"Page {page} has no job listings, stopping")
                break
            
            # Check how many URLs are new vs already seen
            new_urls = current_urls - seen_urls
            duplicate_urls = current_urls & seen_urls
            
            self.logger.info(f"Page {page}: {len(current_urls)} total URLs, {len(new_urls)} new, {len(duplicate_urls)} duplicates")
            
            # If most URLs are duplicates, this page is likely repeating content
            if len(duplicate_urls) > len(new_urls) and page > 1:
                self.logger.info(f"Page {page} has mostly duplicate content, stopping at page {page-1}")
                return page - 1
            
            # If we find fewer URLs than expected and it's not page 1, this might be the last page
            if len(current_urls) < self.page_size and page > 1:
                self.logger.info(f"Page {page} has {len(current_urls)} URLs (less than {self.page_size}), this is the last page")
                seen_urls.update(new_urls)
                return page
            
            seen_urls.update(new_urls)
            page += 1
            
            # Add a small delay between requests
            time.sleep(1)
        
        final_page = min(page - 1, self.max_pages)
        self.logger.info(f"Detected {final_page} actual pages with {len(seen_urls)} unique job URLs")
        return final_page

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
            salary_min, salary_max = None, None
            if salary_elem:
                salary_text = salary_elem.get_text().replace(" ", "") # Remove spaces from numbers
                nums = re.findall(r"\d+", salary_text)
                if len(nums) >= 2:
                    salary_min, salary_max = int(nums[0]), int(nums[1])
                elif nums:
                    salary_min = int(nums[0])
                    salary_max = salary_min

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
        """Scrape job listings with proper duplicate detection between pages."""
        self.logger.info("Starting Warsaw-only scrape with duplicate detection.")
        all_jobs: List[JobListing] = []
        seen_urls: Set[str] = set()

        # Detect the actual number of pages with unique content
        total_pages = self._detect_actual_pages()
        self.logger.info(f"Total pages to scrape: {total_pages}")

        if total_pages == 0:
            self.logger.warning("No pages detected, trying to scrape first page only")
            total_pages = 1

        # Loop through the actual number of pages
        for page in range(1, total_pages + 1):
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching list page {page}/{total_pages}: {page_url}")
            html = self.get_page_html(page_url)
            
            if not html:
                self.logger.warning(f"No HTML for list page {page}, skipping.")
                continue

            current_urls = self._extract_job_urls_from_page(html)
            
            if not current_urls:
                self.logger.warning(f"No listings found on page {page}")
                continue

            # Filter out URLs we've already seen
            new_urls = current_urls - seen_urls
            duplicate_count = len(current_urls) - len(new_urls)
            
            self.logger.info(f"Page {page}: Found {len(current_urls)} total URLs, {len(new_urls)} new, {duplicate_count} duplicates")
            
            if not new_urls:
                self.logger.info(f"Page {page} has no new URLs, stopping pagination")
                break

            # Add new URLs to seen set
            seen_urls.update(new_urls)

            # Process only new URLs
            tasks = [{"url": self.base_url + href} for href in new_urls]
            self.logger.info(f"Page {page}: Processing {len(tasks)} new listings.")

            with ThreadPoolExecutor(max_workers=8) as executor:
                future_to_task = {executor.submit(self.get_page_html, task["url"]): task for task in tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        detail_html = future.result()
                        if detail_html:
                            job = self._parse_job_detail(detail_html, task["url"])
                            if job:
                                all_jobs.append(job)
                    except Exception as exc:
                        self.logger.error(f'{task["url"]} generated an exception: {exc}')
            
            # A polite pause between fetching list pages
            time.sleep(random.uniform(1, 2))
            
        self.logger.info(f"Scrape complete: {len(all_jobs)} total unique jobs found.")
        return all_jobs


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Attempted to insert {len(jobs)} jobs.")
    return jobs
