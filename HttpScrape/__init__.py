import logging
import azure.functions as func
import json
from datetime import datetime

from scraper import fetch_listings
from models import JobListing
from database import insert_job_listing, create_tables_if_not_exist

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function started')
    
    # Initialize database tables
    try:
        logging.info('Creating tables if not exist...')
        create_tables_if_not_exist()
        logging.info('Database initialization complete')
    except Exception as e:
        logging.error(f'Database initialization failed: {e}')
        return func.HttpResponse(f'Database init error: {e}', status_code=500)

    # 1. Fetch listings
    try:
        logging.info('Calling fetch_listings()')
        listings_data = fetch_listings()
        logging.info(f'Fetched {len(listings_data)} listings')
        
        if not listings_data:
            logging.warning('No listings found')
            return func.HttpResponse('No listings found', status_code=200)
            
    except Exception as e:
        logging.error(f'Error fetching listings: {e}')
        import traceback
        logging.error(traceback.format_exc())
        return func.HttpResponse(f'Error fetching: {e}', status_code=500)

    # 2. Build result list
    results = []
    for i, data in enumerate(listings_data):
        try:
            logging.info(f'Processing listing {i+1}/{len(listings_data)}: {data.get("title", "Unknown")}')
            
            listing = JobListing(
                job_id=data['link'],
                source='theprotocol.it',
                title=data['title'],
                company=data['company'],
                link=data['link'],
                scrape_date=datetime.utcnow()
            )

            # Database write
            try:
                short_id = insert_job_listing(listing)
                listing.short_id = short_id
                logging.info(f'Inserted listing with ID: {short_id}')
            except Exception as db_err:
                logging.error(f'DB error for listing {i+1}: {db_err}')
                import traceback
                logging.error(traceback.format_exc())

            results.append({
                'title': listing.title,
                'company': listing.company,
                'link': listing.link,
                'id': getattr(listing, 'short_id', None)
            })
            
        except Exception as e:
            logging.error(f'Error processing listing {i+1}: {e}')
            continue

    logging.info(f'Processed {len(results)} listings successfully')

    # 3. Return JSON response
    return func.HttpResponse(
        body=json.dumps({'results': results, 'count': len(results)}, ensure_ascii=False),
        status_code=200,
        mimetype='application/json'
    )
