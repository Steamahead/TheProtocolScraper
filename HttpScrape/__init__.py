import logging
import azure.functions as func
import json
from datetime import datetime

from scraper import fetch_listings
from models import JobListing
# from database import insert_job_listing  # Disabled for testing


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function started')

    # 1. Fetch listings
    try:
        logging.info('Calling fetch_listings()')
        listings_data = fetch_listings()
        logging.info(f'Fetched {len(listings_data)} listings')
    except Exception as e:
        logging.error(f'Error fetching listings: {e}')
        return func.HttpResponse(f'Error fetching: {e}', status_code=500)

    # 2. Build result list (DB writes commented out)
    results = []
    for data in listings_data:
        listing = JobListing(
            job_id=data['link'],
            source='theprotocol.it',
            title=data['title'],
            company=data['company'],
            link=data['link'],
            scrape_date=datetime.utcnow()
        )

        # Database write is disabled to isolate scraping errors
        try:
             short_id = insert_job_listing(listing)
             listing.short_id = short_id
             logging.info(f'Inserted listing with ID: {short_id}')
        except Exception as db_err:
             logging.error(f'DB error: {db_err}')

        results.append({
            'title': listing.title,
            'company': listing.company,
            'id': getattr(listing, 'short_id', None)
        })

    # 3. Return JSON response
    return func.HttpResponse(
        body=json.dumps(results, ensure_ascii=False),
        status_code=200,
        mimetype='application/json'
    )
