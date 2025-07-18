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
import urllib.parse

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

    def _investigate_pagination_format(self, html: str) -> List[str]:
        """Investigate how pagination works on this website."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for pagination links
        pagination_links = []
        
        # Common pagination selectors
        pagination_selectors = [
            'a[href*="page"]',
            '.pagination a',
            '.pager a',
            'nav a',
            'a[class*="page"]',
            'a[class*="next"]',
            'a[href*="stron"]',  # Polish word for page
            'a[href*=";page"]',
            'a[href*="/page/"]',
            'a[href*="-page-"]'
        ]
        
        for selector in pagination_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if href and ('page' in href.lower() or 'stron' in href.lower() or text.isdigit()):
                    pagination_links.append(f"Text: '{text}' -> URL: '{href}'")
        
        # Also look for any form elements that might handle pagination
        forms = soup.find_all('form')
        for form in forms:
            action = form.get('action', '')
            method = form.get('method', 'GET').upper()
            inputs = form.find_all('input')
            if inputs:
                pagination_links.append(f"FORM {method} -> {action} with {len(inputs)} inputs")
        
        # Look for JavaScript pagination clues
        scripts = soup.find_all('script')
        for script in scripts:
            script_text = script.get_text()
            if 'page' in script_text.lower() and ('next' in script_text.lower() or 'pagination' in script_text.lower()):
                # Extract relevant parts
                lines = [line.strip() for line in script_text.split('\n') if 'page' in line.lower()][:3]
                if lines:
                    pagination_links.append(f"JS: {'; '.join(lines)}")
        
        return pagination_links

    def _try_different_pagination_formats(self, base_url: str, page_num: int) -> List[str]:
        """Try different pagination URL formats."""
        formats = [
            f"{base_url}?page={page_num}",
            f"{base_url};page/{page_num}",
            f"{base_url}/page/{page_num}",
            f"{base_url}?p={page_num}",
            f"{base_url};p/{page_num}",
            f"{base_url}?strona={page_num}",  # Polish for page
            f"{base_url};strona/{page_num}",
            f"{base_url}-page-{page_num}",
            f"{base_url}/strona/{page_num}",
        ]
        return formats

    def _extract_job_urls_from_page(self, html: str) -> Set[str]:
        """Extract unique job URLs from a page."""
        soup = BeautifulSoup(html, 'html.parser')
        hrefs = {a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']}
        return hrefs

    def _detect_working_pagination_format(self) -> tuple[str, int]:
        """Find the correct pagination format and number of pages."""
        self.logger.info("Investigating pagination format...")
        
        # Get first page and investigate pagination
        first_page_html = self.get_page_html(self.search_url)
        if not first_page_html:
            return "", 0
        
        first_page_urls = self._extract_job_urls_from_page(first_page_html)
        self.logger.info(f"First page has {len(first_page_urls)} job URLs")
        
        # Investigate pagination structure
        pagination_info = self._investigate_pagination_format(first_page_html)
        self.logger.info("Pagination investigation results:")
        for info in pagination_info[:10]:  # Log first 10 findings
            self.logger.info(f"  {info}")
        
        # Try different pagination formats for page 2
        formats_to_try = self._try_different_pagination_formats(self.search_url, 2)
        
        for format_url in formats_to_try:
            self.logger.info(f"Testing pagination format: {format_url}")
            
            html = self.get_page_html(format_url)
            if html:
                current_urls = self._extract_job_urls_from_page(html)
                new_urls = current_urls - first_page_urls
                
                self.logger.info(f"  Found {len(current_urls)} URLs, {len(new_urls)} new ones")
                
                if len(new_urls) > 0:
                    self.logger.info(f"  SUCCESS! Found working pagination format: {format_url}")
                    # Now find total pages with this format
                    total_pages = self._count_pages_with_format(format_url.replace("2", "{page}"))
                    return format_url.replace("2", "{page}"), total_pages
            
            time.sleep(1)  # Be polite between requests
        
        self.logger.warning("No working pagination format found, defaulting to single page")
        return "", 1

    def _count_pages_with_format(self, url_template: str) -> int:
        """Count pages using the detected URL format."""
        seen_urls: Set[str] = set()
        page = 1
        
        while page <= self.max_pages:
            page_url = url_template.format(page=page)
            self.logger.info(f"Testing page {page}: {page_url}")
            
            html = self.get_page_html(page_url)
            if not html:
                break
                
            current_urls = self._extract_job_urls_from_page(html)
            
            if not current_urls:
                self.logger.info(f"Page {page} has no job listings")
                break
            
            new_urls = current_urls - seen_urls
            
            if len(new_urls) == 0 and page > 1:
                self.logger.info(f"Page {page} has no new URLs, stopping at page {page-1}")
                return page - 1
            
            if len(current_urls) < self.page_size and page > 1:
                self.logger.info(f"Page {page} has {len(current_urls)} URLs (less than {self.page_size}), this is the last page")
                return page
            
            seen_urls.update(new_urls)
            page += 1
            time.sleep(1)
        
        return min(page - 1, self.max_pages)

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
        """Scrape job listings with automatic pagination format detection."""
        self.logger.info("Starting Warsaw-only scrape with pagination format detection.")
        all_jobs: List[JobListing] = []
        seen_urls: Set[str] = set()

        # Detect the correct pagination format and page count
        url_template, total_pages = self._detect_working_pagination_format()
        
        if not url_template:
            # Fallback to single page
            self.logger.info("Using single page fallback")
            url_template = self.search_url
            total_pages = 1
        
        self.logger.info(f"Using URL template: {url_template}")
        self.logger.info(f"Total pages to scrape: {total_pages}")

        # Loop through the pages
        for page in range(1, total_pages + 1):
            if url_template == self.search_url:
                page_url = self.search_url  # Single page
            else:
                page_url = url_template.format(page=page)
            
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
            
            if not new_urls and page > 1:
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
