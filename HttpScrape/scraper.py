import logging
import re
from datetime import datetime
from typing import List, Optional, Set, Tuple
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

# --- Corrected relative imports ---
from .models import JobListing, Skill
from .database import get_sql_connection, insert_job_listing, insert_skill
from .base_scraper import BaseScraper

class TheProtocolScraper(BaseScraper):
    """Scraper for theprotocol.it job board for all of Poland."""

    def __init__(self):
        super().__init__()
        self.base_url = "https://theprotocol.it"
        self.search_url = (
            "https://theprotocol.it/filtry/big-data-science;"
            "sp/trainee,assistant,junior,mid;p"
        )
        self.num_pages_to_scrape = 6
        
        # --- Skill Categories ---
        self.skill_categories = {
            "Database": ["sql", "mysql", "postgresql", "oracle", "nosql", "mongodb", "database", "ms access", "sqlite", "redshift", "snowflake", "microsoft sql server", "teradata", "clickhouse", "azure sql database", "azure sql managed instance", "mariadb", "ms sql", "sql i pl/sql", "oracle forms", "oracle apex", "oracle ebs", "oracle application framework (oaf)", "oracle erp cloud", "sql server", "mssqlserver", "azure sql", "pl/pgsql", "aas", "neteza", "singlestore", "oracle fusion middleware", "oracle jdeveloper"],
            "Microsoft BI & Excel": ["excel", "power query", "power pivot", "vba", "macros", "pivot tables", "excel formulas", "spreadsheets", "m code", "ssrs", "ssis", "ssas", "power apps", "power automate", "powerpoint", "office 365", "microsoft power bi", "power bi", "power bi.", "ms office", "ms excel", "microsoft dynamics 365", "ms fabric"],
            "Visualization": ["tableau", "qlik", "looker", "data studio", "powerbi", "dax", "matplotlib", "seaborn", "plotly", "excel charts", "dashboard", "reporting", "d3.js", "grafana", "kibana", "google charts", "quicksight", "sas viya", "di studio", "eg", "sas studio", "visual analytics", "qliksense", "sas va", "qgis", "visio"],
            "Programming": ["python", "r", "java", "scala", "c#", ".net", "javascript", "typescript", "pandas", "numpy", "jupyter", "scikit-learn", "tidyverse", "julia", "sql scripting", "pl/sql", "t-sql", "linux", "windows", "unix", "windows server", "macos", "shell", "perl", "pyspark", "go", "rust", "c++", "c", "jee", "scala 3", "next.js", "fastapi", "rest", "spring framework", "css", "html", "u-boot", "yocto", "sas4gl", "mql5", "xml", "uml", "bpmn", "golang", "graphql", "spring boot", "hibernate", "flask api", "pytest", "junit", "liquibase", "jest", "angular", "vue.js", "ngrx", "swagger"],
            "Data Processing": ["etl", "spark", "hadoop", "kafka", "airflow", "data engineering", "big data", "data cleansing", "data transformation", "data modeling", "data warehouse", "databricks", "dbt", "talend", "informatica", "apache spark", "starrocks", "iceberg", "bigquery", "matillion", "data built tool", "apache airflow", "data lake", "adf", "azure data factory", "azure data lake", "parquet", "dwh", "elt/elt", "apache kafka", "alteryx", "azure databricks", "synapse analytics", "informatica cloud"],
            "Analytics & Statistics": ["statistics", "regression", "forecasting", "analytics", "analysis", "spss", "sas", "stata", "hypothesis testing", "a/b testing", "statistical", "time series", "clustering", "segmentation", "correlation", "adobe analytics", "google analytics", "sas di", "sas eg", "sas 4gl", "sas macro language", "data science", "data analytics"],
            "Cloud": ["aws", "azure", "gcp", "google cloud", "cloud", "onedrive", "sharepoint", "snowflake", "lambda", "s3", "pub/sub", "dataflow", "terraform", "google cloud services (big query)", "microsoft azure", "snowflake data cloud", "google cloud platform", "sap datasphere", "azure synapse", "azure functions", "azure repos", "microsoft  azure", "redis", "azure event hub", "ansible", "terragrunt", "vertex ai", "sagemaker", "azure devops"],
            "Business Intelligence": ["business intelligence", "bi", "cognos", "business objects", "microstrategy", "olap", "data mart", "reporting", "kpi", "metrics", "domo", "sisense", "bi publisher", "mis"],
            "Machine Learning and AI": ["machine learning", "scikit-learn", "tensorflow", "keras", "pytorch", "deep learning", "xgboost", "lightgbm", "nlp", "computer vision", "anomaly detection", "feature engineering", "opencv", "langchain", "pydantic", "langgraph", "hugging face ml tools", "mlops", "dagster", "llm", "ai", "ml", "transformers", "openai api", "tensorrt", "seldon", "onnx", "cap’n proto", "llamaindex", "mlflow", "kubeflow", "vllm", "pinecone", "faiss", "chroma", "llm/nlp", "sciklit-learn", "palantir foundry"],
            "Data Governance and Quality": ["data governance", "data quality", "data integrity", "data validation", "master data management", "metadata", "data lineage", "data catalog", "atlan", "collibra", "cdi", "cai", "cdgc"],
            "Data Privacy and Security": ["data privacy", "gdpr", "data security", "compliance", "pii", "data anonymization"],
            "Project Management and Soft Skills": ["project management", "agile", "scrum", "communication", "presentation", "storytelling", "collaboration", "stakeholder management", "requirements gathering", "jira", "confluence", "agile methodologies", "servicenow", "bugzilla", "otrs"],
            "Version Control": ["git", "github", "gitlab", "bitbucket", "svn"],
            "Data Integration and APIs": ["api", "rest api", "data integration", "web scraping", "etl tools", "soap", "ip rotation services", "google python apis", "rest apis", "soapui", "oracle service bus", "oracle soa"],
            "ERP and CRM Systems": ["sap", "oracle", "salesforce", "dynamics", "erp", "crm", "workday"],
            "DevOps": ["jenkins", "openshift", "docker", "kubernetes", "bamboo", "ci/cd", "maven", "gradle", "sonarqube", "argocd", "jenkins / ansible", "controlm", "liquiibase", "sonar"]
        }

    def _parse_skills(self, soup: BeautifulSoup) -> List[Tuple[str, str]]:
        """Extracts skills and their categories from the job detail page, skipping any not in predefined categories."""
        found_skills = []
        skill_elements = soup.select('div[data-test="chip-technology"]')
        for elem in skill_elements:
            skill_name = elem.get('title', '').strip()
            if not skill_name:
                continue
            normalized_skill = skill_name.lower()
            for cat, skills_in_cat in self.skill_categories.items():
                if normalized_skill in skills_in_cat:
                    found_skills.append((skill_name, cat))
                    break
        return found_skills

    def _parse_years_of_experience(self, soup: BeautifulSoup) -> Optional[int]:
        """Extracts years of experience from the requirements list."""
        try:
            requirements = soup.select('li.lxul5ps')
            for req in requirements:
                text = req.get_text(strip=True).lower()
                if 'rynku' in text or 'firmy' in text: continue
                match = re.search(r'(?<![a-z])(\d+)\+?\s*(?:lata|lat|letnie|year|years)', text)
                if match:
                    years = int(match.group(1))
                    if years <= 8:
                        return years
        except Exception:
            return None
        return None

    def _parse_job_detail(self, html: str, job_url: str) -> Optional[Tuple[JobListing, List[Tuple[str, str]]]]:
        """Parses job details and skills, returning them as a tuple."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            title = soup.select_one('h1[data-test="text-offerTitle"]').get_text(strip=True)
            company = soup.select_one('a[data-test="anchor-company-link"]').get_text(strip=True).split(':')[-1].strip()
            operating_mode = soup.select_one('span[data-test="content-workModes"]').get_text(strip=True)
            location = soup.select_one('span[data-test="text-primaryLocation"]').get_text(strip=True)
            contract_text = soup.select_one('span[data-test="text-contractName"]').get_text(strip=True)
            m = re.search(r"\(([^)]+)\)", contract_text)
            work_type = m.group(1) if m else contract_text or "N/A"
            experience = soup.select_one('span[data-test="content-positionLevels"]').get_text(separator=", ", strip=True).replace('•', ',')
            
            salary_elem = soup.select_one('span[data-test="text-contractSalary"]')
            salary_min, salary_max = None, None
            if salary_elem:
                salary_text = salary_elem.get_text().replace('\xa0', '').replace(' ', '')
                nums = re.findall(r'\d+', salary_text)
                if len(nums) >= 2:
                    salary_min, salary_max = int(nums[0]), int(nums[1])
                elif nums:
                    salary_min = int(nums[0])
                    salary_max = salary_min

            id_elem = soup.select_one('span[data-test="text-offerId"]')
            job_id = ""
            if id_elem and id_elem.get_text(strip=True).isdigit():
                job_id = id_elem.get_text(strip=True)
            else:
                uuid_m = re.search(r',oferta,([a-zA-Z0-9\-]+)', job_url)
                if uuid_m:
                    job_id = uuid_m.group(1)
            
            years_exp = self._parse_years_of_experience(soup)
            skills_data = self._parse_skills(soup)

            job_listing = JobListing(
                job_id=job_id, source='theprotocol.it', title=title, company=company, link=job_url,
                operating_mode=operating_mode, salary_min=salary_min, salary_max=salary_max, location=location,
                work_type=work_type, experience_level=experience, employment_type=work_type,
                years_of_experience=years_exp, scrape_date=datetime.utcnow(), listing_status='Active'
            )
            return job_listing, skills_data
        except Exception as e:
            self.logger.error(f"Error parsing detail {job_url}: {e}", exc_info=False)
            return None

    def scrape_all_data(self) -> List[Tuple[JobListing, List[Tuple[str, str]]]]:
        """Scrapes all job and skill data without touching the database."""
        self.logger.info(f"Starting nationwide scrape for {self.num_pages_to_scrape} pages.")
        all_results: List[Tuple[JobListing, List[Tuple[str, str]]]] = []
        seen_urls: Set[str] = set()

        for page in range(1, self.num_pages_to_scrape + 1):
            page_url = f"{self.search_url}?pageNumber={page}" if page > 1 else self.search_url
            self.logger.info(f"Fetching list page {page}/{self.num_pages_to_scrape}: {page_url}")
            
            html = self.get_page_html(page_url)
            if not html: continue
            
            soup = BeautifulSoup(html, 'html.parser')
            new_urls = {a['href'] for a in soup.find_all('a', href=True) if ',oferta,' in a['href']} - seen_urls
            if not new_urls: continue

            seen_urls.update(new_urls)
            tasks = [{"url": self.base_url + href} for href in new_urls]

            with ThreadPoolExecutor(max_workers=12) as executor:
                future_to_task = {executor.submit(self.get_page_html, task["url"]): task for task in tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        detail_html = future.result()
                        if detail_html:
                            result = self._parse_job_detail(detail_html, task["url"])
                            if result:
                                all_results.append(result)
                    except Exception as exc:
                        self.logger.error(f'Fetching detail page {task["url"]} generated an exception: {exc}')
            
            if page < self.num_pages_to_scrape:
                time.sleep(random.uniform(1, 2))
                    
        self.logger.info(f"Scraping complete: {len(all_results)} total jobs found.")
        return all_results

def run_scraper():
    """Orchestrates the scraping and database insertion."""
    logging.info("Scraper process started.")
    
    # --- Phase 1: Scrape all data ---
    scraper = TheProtocolScraper()
    scraped_data = scraper.scrape_all_data()
    
    if not scraped_data:
        logging.info("No data scraped. Process finished.")
        return
        
    # --- Phase 2: Insert data into database ---
    jobs_inserted = 0
    skills_inserted = 0
    connection = None
    try:
        connection = get_sql_connection()
        logging.info("Database connection opened for batch insert.")
        with connection.cursor() as cursor:
            for job_listing, skills_data in scraped_data:
                new_job_id = insert_job_listing(job_listing, cursor)
                
                if new_job_id:
                    jobs_inserted += 1
                    for skill_name, skill_category in skills_data:
                        skill = Skill(
                            job_id=job_listing.job_id,
                            short_id=new_job_id, 
                            source='theprotocol.it',
                            skill_name=skill_name,
                            skill_category=skill_category
                        )
                        if insert_skill(skill, cursor):
                            skills_inserted += 1
            connection.commit()
            logging.info("All data committed to database.")
    except Exception as e:
        logging.error(f"An error occurred during database insertion: {e}", exc_info=True)
        if connection:
            connection.rollback()
            logging.warning("Database transaction was rolled back.")
    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed.")

    logging.info(f"Process complete. Inserted: {jobs_inserted} jobs and {skills_inserted} skills.")
