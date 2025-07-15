import logging
import azure.functions as func
import json
# --- CORRECTED IMPORTS ---
# These now import from the project root, not a relative path.
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
