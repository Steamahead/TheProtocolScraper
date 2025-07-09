import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import logging

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

URL = (
    "https://theprotocol.it/filtry/big-data-science;"
    "sp/junior,assistant,trainee,mid;"
    "p/warszawa;wp/praca/bi-developer-warszawa-zubra-1,oferta,"  
    "952b0000-9568-9a92-a6a4-08ddba1ba907"
)

def fetch_listings() -> List[Dict[str, str]]:
    """
    Fetches the page and returns a list of dicts with 'title', 'company', and 'link'.
    """
    logging.info(f"Starting scrape of URL: {URL}")
    
    try:
        resp = requests.get(URL, headers=headers, timeout=30)
        resp.raise_for_status()
        logging.info(f"HTTP request successful, status: {resp.status_code}")
    except Exception as e:
        logging.error(f"HTTP request failed: {e}")
        raise

    soup = BeautifulSoup(resp.text, 'html.parser')
    jobs = soup.select('div.offer-card')
    logging.info(f"Found {len(jobs)} job cards in HTML")
    
    results = []

    for i, job in enumerate(jobs):
        title_elem = job.select_one('h2.title')
        company_elem = job.select_one('div.company')
        link_elem = job.select_one('a.offer-link')

        if not title_elem or not company_elem or not link_elem:
            logging.warning(f"Job card {i+1} missing required elements")
            continue

        job_data = {
            'title': title_elem.get_text(strip=True),
            'company': company_elem.get_text(strip=True),
            'link': link_elem['href']
        }
        
        results.append(job_data)
        logging.info(f"Parsed job {i+1}: {job_data['title']} at {job_data['company']}")

    logging.info(f"Successfully parsed {len(results)} jobs")
    return results
