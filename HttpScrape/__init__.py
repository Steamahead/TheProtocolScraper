import logging
import azure.functions as func
import json
import sys
import os

# --- FIX for Import Errors ---
# This adds the root of your project to the system path, ensuring all modules are found.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Original Imports ---
# These will now work correctly.
from scraper import run_scraper
from database import create_tables_if_not_exist

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function started.')

    try:
        logging.info("Ensuring database tables exist...")
        create_tables_if_not_exist()
        logging.info("Database tables are ready.")

        scraped_jobs = run_scraper()
        
        return func.HttpResponse(
            body=json.dumps({
                'status': 'success',
                'scraped_jobs': len(scraped_jobs)
            }),
            status_code=200,
            mimetype='application/json'
        )

    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}", exc_info=True)
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)
