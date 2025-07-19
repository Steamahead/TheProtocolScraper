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
    """Scraper for theprotocol.it job board for all of Poland."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        # URL for nationwide search, which is also page 1
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/trainee,assistant,junior,mid;p"
        )
        self.num_pages_to_scrape = 6

    def _parse_years_of_experience(self, soup: BeautifulSoup) -> Optional[int]:
        """
        Extracts years of experience, capping at 8 years and handling edge cases.
        """
        try:
            requirements = soup.select('li.lxul5ps')
            for req in requirements:
                text = req.get_text(strip=True).lower()

                # Skip sentences about the company's age on the market
                if 'rynku' in text or 'firmy' in text:
                    continue

                # Regex to find a number (e.g., 3, 2+) followed by year-related keywords
                match = re.search(r'(?<![a-z])(\d+)\+?\s*(?:lata|lat|letnie|year|years)', text)
                
                if match:
                    years = int(match.group(1))
                    # If the number is greater than 8, ignore it as requested.
                    if years > 8:
                        continue
                    
                    self.logger.info(f"Found valid years of experience: {years}")
                    return years
        except Exception as e:
            self.logger.error(f"Error parsing years of experience: {e}", exc_info=True)
        return None

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        """Parses the job detail page to extract all relevant information."""
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
                salary_text = salary_elem.get_text().replace(" ", "")
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
            
            years_exp = self._parse_years_of_experience(soup)

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
                years_of_experience=years_exp,
                scrape_date=datetime.utcnow(),
                listing_status='Active'
            )
        except Exception as e:
            self.logger.error(f"Error parsing detail {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[JobListing]:
        """Scrapes job listings from all of Poland for a fixed number of pages."""
        self.logger.info(f"Starting nationwide scrape for {self.num_pages_to_scrape} pages.")
        all_jobs: List[JobListing] = []
        seen_urls: Set[str] = set()

        for page in range(1, self.num_pages_to_scrape + 1):
            # Page 1 has a different URL structure
            if page == 1:
                page_url = self.search_url
            else:
                page_url = f"{self.search_url}?pageNumber={page}"
            
            self.logger.info(f"Fetching list page {page}/{self.num_pages_to_scrape}: {page_url}")
            html = self.get_page_html(page_url)
            
            if not html:
                self.logger.warning(f"No HTML for list page {page}, skipping.")
                continue

            soup = BeautifulSoup(html, 'html.parser')
            current_urls = {a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']}
            
            new_urls = current_urls - seen_urls
            self.logger.info(f"Page {page}: Found {len(new_urls)} new listings.")
            
            if not new_urls:
                continue

            seen_urls.update(new_urls)
            tasks = [{"url": self.base_url + href} for href in new_urls]

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
            
            if page < self.num_pages_to_scrape:
                time.sleep(random.uniform(1, 2))
                    
        self.logger.info(f"Scrape complete: {len(all_jobs)} total unique jobs found.")
        return all_jobs

def run_scraper():
    """Entry point for the Azure Function to run the scraper."""
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    logging.info(f"Attempted to insert {len(jobs)} jobs.")
    return jobs
