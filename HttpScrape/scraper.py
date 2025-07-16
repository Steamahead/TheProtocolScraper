import logging
import re
from datetime import datetime
from typing import List, Optional, Set, Dict
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Corrected relative imports ---
from .models import JobListing, Skill
from .database import insert_job_listing, insert_skill
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board with pagination, unified numeric job IDs, skill extraction, and parallel detail fetching."""

    def __init__(self, max_workers: int = 10):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/junior,assistant,trainee,mid;p/warszawa"
        )
        self.max_workers = max_workers
        # skill categories mapping omitted for brevity (unchanged)
        self.skill_categories: Dict[str, List[str]] = {
            "Database": ["sql", "mysql", "postgresql", "oracle", "nosql", "mongodb"],
            "Visualization": ["power bi", "tableau", "qlik", "grafana"],
            "Cloud": ["azure", "aws", "gcp"],
            "Programming": ["python", "java", "scala", "javascript", "r"],
        }

    def _map_skill(self, raw: str) -> Optional[Skill]:
        text = raw.strip().lower()
        for category, skills in self.skill_categories.items():
            for sk in skills:
                if sk in text:
                    return Skill(job_id="", source="theprotocol.it", skill_name=sk.title(), skill_category=category)
        return None

    def _extract_skills(self, soup: BeautifulSoup) -> Set[str]:
        raw_skills = set()
        for chip in soup.select('div[data-test="chip-technology"]'):
            title = chip.get('title') or chip.get_text(strip=True)
            if title:
                raw_skills.add(title)
        return raw_skills

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[JobListing]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # parse fields (unchanged from previous version)
            title_elem = soup.select_one('h1[data-test="text-offerTitle"]')
            title = title_elem.get_text(strip=True) if title_elem else "N/A"
            company_elem = soup.select_one('a[data-test="anchor-company-link"]')
            company = company_elem.get_text(strip=True).split(':')[-1].strip() if company_elem else "N/A"
            operating_mode = soup.select_one('span[data-test="content-workModes"]').get_text(strip=True) if soup.select_one('span[data-test="content-workModes"]') else "N/A"
            location = soup.select_one('span[data-test="text-primaryLocation"]').get_text(strip=True) if soup.select_one('span[data-test="text-primaryLocation"]') else "N/A"
            # bracketed work type
            cont_text = soup.select_one('span[data-test="text-contractName"]').get_text() if soup.select_one('span[data-test="text-contractName"]') else ""
            work_type_match = re.search(r"\(([^)]+)\)", cont_text)
            work_type = work_type_match.group(1) if work_type_match else cont_text.strip() or "N/A"
            experience_level = soup.select_one('span[data-test="content-positionLevels"]').get_text(separator=", ", strip=True).replace('•', ',') if soup.select_one('span[data-test="content-positionLevels"]') else "N/A"
            salary_text = soup.select_one('span[data-test="text-contractSalary"]').get_text() if soup.select_one('span[data-test="text-contractSalary"]') else ""
            nums = re.findall(r"\d+", salary_text)
            salary_min, salary_max = (int(nums[0]), int(nums[1])) if len(nums) >= 2 else (int(nums[0]), None) if nums else (None, None)
            # numeric job ID if exists
            id_elem = soup.select_one('span[data-test="text-offerId"]')
            if id_elem and id_elem.get_text(strip=True).isdigit():
                job_id = id_elem.get_text(strip=True)
            else:
                uuid_match = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
                job_id = uuid_match.group(1) if uuid_match else job_url

            job = JobListing(
                job_id=job_id, source='theprotocol.it', title=title, company=company,
                link=job_url, salary_min=salary_min, salary_max=salary_max,
                location=location, operating_mode=operating_mode, work_type=work_type,
                experience_level=experience_level, employment_type=work_type,
                years_of_experience=None, scrape_date=datetime.utcnow(), listing_status='Active'
            )
            # insert skills
            soup_skills = self._extract_skills(soup)
            for raw in soup_skills:
                sk_obj = self._map_skill(raw)
                if sk_obj:
                    sk_obj.job_id = job.job_id
                    insert_skill(sk_obj)
            return job
        except Exception as e:
            self.logger.error(f"Error parsing detail {job_url}: {e}", exc_info=True)
            return None

    def scrape(self) -> List[JobListing]:
        self.logger.info("Starting pagination scrape with parallel detail fetch.")
        all_jobs: List[JobListing] = []
        page = 1
        while True:
            url = f"{self.search_url}?page={page}"
            self.logger.info(f"Fetching page {page}")
            html = self.get_page_html(url)
            if not html:
                break
            soup = BeautifulSoup(html, 'html.parser')
            links = [a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']]
            if not links:
                break
            unique_links = list(dict.fromkeys(links))
            self.logger.info(f"Page {page}: {len(unique_links)} listings")

            # parallel fetch details
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.get_page_html, self.base_url + href): href
                    for href in unique_links
                }
                for future in as_completed(future_to_url):
                    href = future_to_url[future]
                    try:
                        detail_html = future.result(timeout=30)
                        if detail_html:
                            job = self._parse_job_detail(detail_html, self.base_url + href)
                            if job:
                                all_jobs.append(job)
                    except Exception as e:
                        self.logger.warning(f"Failed to fetch or parse {href}: {e}")
            page += 1
        self.logger.info(f"Scrape complete: {len(all_jobs)} jobs total.")
        return all_jobs


def run_scraper():
    logging.info("Scraper started.")
    scraper = TheProtocolScraper()
    jobs = scraper.scrape()
    for job in jobs:
        insert_job_listing(job)
    self.logger.info(f"Finished: {len(jobs)} jobs inserted.")
    return jobs
