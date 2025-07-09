import logging
import azure.functions as func
import json
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function started - testing imports')
    
    try:
        logging.info('Testing scraper import...')
        from scraper import fetch_listings
        logging.info('Scraper import successful')
        
        logging.info('Testing models import...')
        from models import JobListing
        logging.info('Models import successful')
        
        logging.info('Testing database import...')
        from database import insert_job_listing, create_tables_if_not_exist
        logging.info('Database import successful')
        
        return func.HttpResponse(
            body=json.dumps({'status': 'success', 'message': 'All imports working'}),
            status_code=200,
            mimetype='application/json'
        )
        
    except Exception as e:
        logging.error(f'Import error: {e}')
        import traceback
        logging.error(traceback.format_exc())
        return func.HttpResponse(f'Import error: {e}', status_code=500)
