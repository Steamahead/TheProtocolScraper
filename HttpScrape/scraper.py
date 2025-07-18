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
import json

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

    def _extract_pagination_from_json(self, html: str) -> Optional[int]:
        """Extract pagination info from the JSON data in the page."""
        try:
            # Look for the JSON data that contains pagination info
            pattern = r'"page":\s*\{\s*"number":\s*\d+,\s*"size":\s*\d+,\s*"count":\s*(\d+)\s*\}'
            match = re.search(pattern, html)
            if match:
                page_count = int(match.group(1))
                self.logger.info(f"Found page count from JSON: {page_count}")
                return page_count
            
            # Alternative pattern for total offers
            pattern2 = r'"offersCount":\s*(\d+)'
            match2 = re.search(pattern2, html)
            if match2:
                total_offers = int(match2.group(1))
                page_count = math.ceil(total_offers / self.page_size)
                self.logger.info(f"Found total offers: {total_offers}, calculated pages: {page_count}")
                return page_count
                
        except Exception as e:
            self.logger.debug(f"Could not extract pagination from JSON: {e}")
        return None

    def _extract_job_urls_from_page(self, html: str) -> Set[str]:
        """Extract unique job URLs from a page."""
        soup = BeautifulSoup(html, 'html.parser')
        hrefs = {a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']}
        return hrefs

    def _debug_page_content(self, page_num: int, html: str, urls: Set[str]):
        """Debug helper to log page content details."""
        # Extract some job titles for debugging
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for job titles in various possible selectors
        title_selectors = [
            'h3[data-test="text-offerTitle"]',
            'h2[data-test="text-offerTitle"]', 
            'a[data-test="anchor-offer-title"]',
            '.offer-title',
            'h3 a',
            'h2 a'
        ]
        
        titles = []
        for selector in title_selectors:
            elements = soup.select(selector)
            for elem in elements[:3]:  # First 3 titles
                title = elem.get_text(strip=True)
                if title and len(title) > 10:  # Valid title
                    titles.append(title[:50])  # Truncate long titles
            if titles:
                break
        
        self.logger.info(f"Page {page_num} debug - URLs: {len(urls)}, Sample titles: {titles[:3]}")
        
        # Log first few URL patterns for debugging
        url_samples = list(urls)[:3]
        self.logger.info(f"Page {page_num} sample URLs: {url_samples}")

    def scrape(self) -> List[JobListing]:
        """Scrape job listings with improved pagination handling."""
        self.logger.info("Starting Warsaw-only scrape with improved pagination detection.")
        all_jobs: List[JobListing] = []
        seen_urls: Set[str] = set()

        # First, get the expected page count from the first page
        first_page_html = self.get_page_html(self.search_url)
        if not first_page_html:
            self.logger.error("Could not fetch the first page. Aborting scrape.")
            return []

        expected_pages = self._extract_pagination_from_json(first_page_html)
        if not expected_pages:
            expected_pages = 4  # Fallback based on your observation

        self.logger.info(f"Expected pages to scrape: {expected_pages}")

        # Process each page, regardless of duplicate detection issues
        for page in range(1, expected_pages + 1):
            if page == 1:
                page_url = self.search_url
            else:
                page_url = f"{self.search_url}-page-{page}"
            
            self.logger.info(f"Fetching list page {page}/{expected_pages}: {page_url}")
            
            # Add some randomization to avoid being blocked
            time.sleep(random.uniform(2, 4))
            
            html = self.get_page_html(page_url)
            
            if not html:
                self.logger.warning(f"No HTML for list page {page}, skipping.")
                continue

            current_urls = self._extract_job_urls_from_page(html)
            
            if not current_urls:
                self.logger.warning(f"No listings found on page {page}")
                # Don't break here, continue to next page
                continue

            # Debug the page content
            self._debug_page_content(page, html, current_urls)

            # Filter out URLs we've already seen
            new_urls = current_urls - seen_urls
            duplicate_count = len(current_urls) - len(new_urls)
            
            self.logger.info(f"Page {page}: Found {len(current_urls)} total URLs, {len(new_urls)} new, {duplicate_count} duplicates")
            
            # Don't stop even if no new URLs - continue to check remaining pages
            if not new_urls:
                self.logger.warning(f"Page {page} has no new URLs, but continuing to check remaining pages...")
                continue

            # Add new URLs to seen set
            seen_urls.update(new_urls)

            # Process only new URLs
            tasks = [{"url": self.base_url + href} for href in new_urls]
            self.logger.info(f"Page {page}: Processing {len(tasks)} new listings.")

            with ThreadPoolExecutor(max_workers=6) as executor:  # Reduced concurrency
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
            
            self.logger.info(f"Page {page} complete. Total unique jobs so far: {len(all_jobs)}")
            
        self.logger.info(f"Scrape complete: {len(all_jobs)} total unique jobs found across {expected_pages} pages.")
        self.logger.info(f"Total unique URLs collected: {len(seen_urls)}")
        
        return all_jobs

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


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Attempted to insert {len(jobs)} jobs.")
    return jobs
