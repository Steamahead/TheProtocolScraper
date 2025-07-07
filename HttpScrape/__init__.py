import logging
import azure.functions as func
import requests
from bs4 import BeautifulSoup
from models import JobListing  # Ensure models.py is at root
from database import insert_job_listing  # Ensure database.py is at root
import json
from datetime import datetime

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing scrape request...')

    # 1. URL to fetch
    url = (
        "https://theprotocol.it/filtry/big-data-science;"
        "sp/junior,assistant,trainee,mid;"
        "p/warszawa;wp/praca/bi-developer-warszawa-zubra-1,oferta,"  
        "952b0000-9568-9a92-a6a4-08ddba1ba907"
    )

    # 2. Set headers to mimic a browser
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    # 3. Fetch page with headers
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Fetch error: {e}")
        return func.HttpResponse(f"Error fetching page: {e}", status_code=500)

    # 4. Parse title as a quick check
    soup = BeautifulSoup(resp.text, 'html.parser')

    listings = []
    jobs = soup.select('div.offer-card')  # adjust selector
    for job in jobs:
        title = job.select_one('h2.title').get_text(strip=True)
        company = job.select_one('div.company').get_text(strip=True)
        link = job.select_one('a.offer-link')['href']

        listing = JobListing(
            job_id=link,
            source='theprotocol.it',
            title=title,
            company=company,
            link=link,
            salary_min=None,
            salary_max=None,
            location='',
            operating_mode='',
            work_type='',
            experience_level='',
            employment_type='',
            years_of_experience=None,
            scrape_date=datetime.utcnow(),
            listing_status='Active'
        )
        insert_job_listing(listing)
        listings.append({'title': title, 'company': company})

    return func.HttpResponse(
        body=json.dumps(listings, ensure_ascii=False),
        status_code=200,
        mimetype='application/json'
    )
    title = soup.title.string if soup.title else 'No title found'

    # 5. Return result
    return func.HttpResponse(f"Fetched successfully! Title: {title}")
