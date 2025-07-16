import logging
import re
from datetime import datetime
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException

# --- CORRECTED RELATIVE IMPORTS ---
from .models import JobListing, Skill
from .database import insert_job_listing, insert_skill
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = "https://theprotocol.it/filtry/big-data-science;sp/junior,assistant,trainee,mid;p/warszawa"

    def get_page_html_with_selenium(self, url: str) -> str:
        """Get HTML content from a URL using Selenium, with enhanced error handling."""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        driver = webdriver.Chrome(options=chrome_options)
        try:
            logging.info(f"Selenium is navigating to: {url}")
            driver.get(url)
            # --- INCREASED TIMEOUT AND ADDED MORE SPECIFIC WAIT CONDITION ---
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-test="link-offer"]'))
            )
            logging.info("Selenium successfully found job links on the page.")
            return driver.page_source
        except TimeoutException:
            # --- NEW: Log the page source if the timeout occurs for debugging ---
            logging.error("TimeoutException: The page did not load the expected elements in time.")
            logging.error("--- Page Source at time of Timeout ---")
            logging.error(driver.page_source)
            logging.error("--- End of Page Source ---")
            # Re-raise the exception to be handled by the main function
            raise
        finally:
            driver.quit()

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[Tuple[JobListing, List[str]]]:
        """
        Parses the HTML of a single job listing page.
        Returns a tuple containing the JobListing object and a list of skill strings.
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.select_one('h1[data-test="text-offerTitle"]').get_text(strip=True)
            company_elem = soup.select_one('a[data-test="anchor-company-link"]')
            company = "N/A"
            if company_elem:
                full_text = company_elem.get_text(strip=True)
                company = full_text.split(':')[-1].strip() if ':' in full_text else full_text

            operating_mode = soup.select_one('span[data-test="content-workModes"]').get_text(strip=True)
            job_id_match = re.search(r'oferta,([a-zA-Z0-9\-]+)$', job_url)
            job_id = job_id_match.group(1) if job_id_match else job_url

            job = JobListing(
                job_id=job_id, source='theprotocol.it', title=title, company=company,
                link=job_url, operating_mode=operating_mode
            )

            skills = []
            requirements_section = soup.select_one('div[data-test="section-requirements"]')
            if requirements_section:
                skill_elements = requirements_section.select('ul > li')
                for skill_elem in skill_elements:
                    skills.append(skill_elem.get_text(strip=True))
            
            logging.info(f"Successfully parsed job: {job.title}. Found {len(skills)} skills.")
            return job, skills
        except Exception as e:
            self.logger.error(f"Error parsing detail page {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[Tuple[JobListing, List[str]]]:
        """Main scraping method. Returns a list of tuples (JobListing, skills)."""
        self.logger.info(f"Fetching listings from {self.search_url}")
        listings_html = self.get_page_html_with_selenium(self.search_url)
        if not listings_html:
            self.logger.error("Failed to fetch the main listings page.")
            return []
            
        soup = BeautifulSoup(listings_html, 'html.parser')
        job_links = soup.select('a[data-test="link-offer"]')
        
        all_job_data = []
        for link_elem in job_links:
            job_url = self.base_url + link_elem['href']
            detail_html = self.get_page_html(job_url)
            if detail_html:
                parsed_data = self._parse_job_detail(detail_html, job_url)
                if parsed_data:
                    all_job_data.append(parsed_data)
                    
        self.logger.info(f"Scrape complete. Found {len(all_job_data)} total jobs.")
        return all_job_data

def run_scraper():
    """Initializes and runs the scraper, and saves job and skills to the database."""
    logging.info("Scraper process started.")
    scraper = TheProtocolScraper()
    jobs_with_skills = scraper.scrape()
    
    processed_count = 0
    for job, skills in jobs_with_skills:
        job_db_id = insert_job_listing(job)
        
        if job_db_id and skills:
            job.short_id = job_db_id
            logging.info(f"Inserting {len(skills)} skills for job ID {job.job_id}")
            for skill_name in skills:
                skill = Skill(
                    job_id=job.job_id,
                    short_id=job.short_id,
                    source=job.source,
                    skill_name=skill_name,
                    skill_category='Requirement'
                )
                insert_skill(skill)
        processed_count += 1
        
    logging.info(f"Scraping finished. {processed_count} jobs processed.")
    return jobs_with_skills
