import logging
import re
from datetime import datetime
from typing import List, Optional
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

    def _get_total_pages(self, html: str) -> int:
        """Parses the total number of job offers to determine the number of pages."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Try multiple selectors to find the total number of offers
        selectors_to_try = [
            'h2.results-header__title > span',
            'h2.results-header__title span',
            '.results-header__title > span',
            '.results-header__title span',
            'h2[class*="results"] span',
            'span[class*="results"]',
            '.results-count',
            '[data-test*="results"]',
            'h2 span'
        ]
        
        for selector in selectors_to_try:
            try:
                total_offers_text = soup.select_one(selector)
                if total_offers_text:
                    text = total_offers_text.get_text(strip=True)
                    self.logger.info(f"Found element with selector '{selector}': '{text}'")
                    
                    # Extract numbers from the text
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        total_offers = int(numbers[0])
                        if total_offers > 0:
                            pages = math.ceil(total_offers / self.page_size)
                            self.logger.info(f"Calculated {pages} pages from {total_offers} total offers")
                            return min(pages, self.max_pages)
            except Exception as e:
                self.logger.debug(f"Selector '{selector}' failed: {e}")
                continue
        
        # Try to find pagination elements
        return self._get_pages_from_pagination(soup)

    def _get_pages_from_pagination(self, soup: BeautifulSoup) -> int:
        """Try to determine total pages from pagination elements."""
        try:
            # Look for pagination links
            pagination_selectors = [
                '.pagination a',
                '[class*="pagination"] a',
                '[class*="pager"] a',
                'a[href*="page="]',
                'nav a[href*="page="]'
            ]
            
            max_page = 0
            for selector in pagination_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href', '')
                    page_match = re.search(r'page=(\d+)', href)
                    if page_match:
                        page_num = int(page_match.group(1))
                        max_page = max(max_page, page_num)
                        
                    # Also check link text for page numbers
                    text = link.get_text(strip=True)
                    if text.isdigit():
                        page_num = int(text)
                        max_page = max(max_page, page_num)
            
            if max_page > 0:
                self.logger.info(f"Found {max_page} pages from pagination elements")
                return min(max_page, self.max_pages)
        except Exception as e:
            self.logger.debug(f"Pagination detection failed: {e}")
        
        # If we can't determine the total, try scraping a few pages
        return self._detect_pages_by_testing()

    def _detect_pages_by_testing(self) -> int:
        """Test pages sequentially to find the actual number of pages."""
        self.logger.info("Testing pages to find total count...")
        
        for page in range(1, self.max_pages + 1):
            try:
                test_url = f"{self.search_url}?page={page}"
                html = self.get_page_html(test_url)
                
                if not html:
                    self.logger.info(f"Page {page} returned no content, stopping at page {page-1}")
                    return max(1, page - 1)
                
                soup = BeautifulSoup(html, 'html.parser')
                hrefs = [a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
                
                if not hrefs:
                    self.logger.info(f"Page {page} has no job listings, stopping at page {page-1}")
                    return max(1, page - 1)
                    
                if len(hrefs) < self.page_size:
                    self.logger.info(f"Page {page} has {len(hrefs)} listings (less than {self.page_size}), this is the last page")
                    return page
                    
                self.logger.info(f"Page {page} has {len(hrefs)} listings, continuing...")
                
            except Exception as e:
                self.logger.error(f"Error testing page {page}: {e}")
                return max(1, page - 1)
        
        self.logger.warning(f"Reached maximum page limit ({self.max_pages})")
        return self.max_pages

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
        """Paginates Warsaw-only listings concurrently with improved page detection."""
        self.logger.info("Starting Warsaw-only pagination scrape with concurrency.")
        all_jobs: List[JobListing] = []

        # Get the total number of pages from the first page of results.
        first_page_html = self.get_page_html(self.search_url)
        if not first_page_html:
            self.logger.error("Could not fetch the first page. Aborting scrape.")
            return []

        # Debug: log a snippet of the HTML to understand the structure
        self.logger.info(f"First page HTML length: {len(first_page_html)}")
        soup_debug = BeautifulSoup(first_page_html, 'html.parser')
        h2_elements = soup_debug.find_all('h2')
        for i, h2 in enumerate(h2_elements[:5]):  # Log first 5 h2 elements
            self.logger.info(f"H2 element {i}: {h2.get_text(strip=True)[:100]}")

        total_pages = self._get_total_pages(first_page_html)
        self.logger.info(f"Total pages to scrape: {total_pages}")

        # Loop through the known number of pages.
        for page in range(1, total_pages + 1):
            page_url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching list page {page}/{total_pages}: {page_url}")
            html = self.get_page_html(page_url)
            
            if not html:
                self.logger.warning(f"No HTML for list page {page}, skipping.")
                continue

            soup = BeautifulSoup(html, 'html.parser')
            hrefs = {a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']}
            
            if not hrefs:
                self.logger.warning(f"No listings found on page {page}. This might be the end.")
                if page > 1:  # If it's not the first page, we can break
                    break
                continue

            tasks = [{"url": self.base_url + href} for href in hrefs]
            self.logger.info(f"Page {page}: Found {len(tasks)} listings to process.")

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
            
            # A polite pause between fetching list pages.
            time.sleep(random.uniform(1, 2))
            
        self.logger.info(f"Scrape complete: {len(all_jobs)} total jobs found.")
        return all_jobs


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Attempted to insert {len(jobs)} jobs.")
    return jobs
