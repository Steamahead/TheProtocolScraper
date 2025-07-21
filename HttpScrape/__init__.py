import logging
import azure.functions as func
import json
from datetime import datetime

# --- CORRECTED RELATIVE IMPORTS ---
from .scraper import run_scraper
from .database import create_tables_if_not_exist

def main(myTimer: func.TimerRequest) -> None:
    """
    Timer-triggered function to run the scraper daily.
    """
    utc_timestamp = datetime.utcnow().isoformat()
    logging.info(f'HttpScrape timer trigger function ran at: {utc_timestamp}')

    if myTimer.past_due:
        logging.info('The timer is past due!')

    try:
        logging.info("Ensuring database tables exist...")
        create_tables_if_not_exist()
        logging.info("Database tables are ready.")

        # Run the scraper
        run_scraper()
        
        logging.info(f'Scraper function finished successfully at: {datetime.utcnow().isoformat()}')

    except Exception as e:
        logging.error(f"An error occurred in the main timer function: {e}", exc_info=True)
