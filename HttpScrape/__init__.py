import logging
from datetime import datetime
import azure.functions as func

# --- Relative imports for scraper and database functions ---
from .scraper import run_scraper
from .database import create_tables_if_not_exist

def main(myTimer: func.TimerRequest) -> None:
    """
    Azure Function triggered by a timer to run the web scraper.

    This function initializes the database, runs the scraper, and logs the process.
    """
    
    # Log the function execution time
    utc_timestamp = datetime.utcnow().isoformat()
    logging.info(f"🚀 Python timer trigger function executed at: {utc_timestamp}")

    # Check if the timer invocation is late
    if myTimer.past_due:
        logging.warning("The timer is past due! This may indicate a scaling issue.")

    try:
        # Step 1: Ensure database tables are created
        logging.info("Initializing database...")
        create_tables_if_not_exist()
        logging.info("✅ Database is ready.")

        # Step 2: Run the main scraper function
        logging.info("Starting the web scraping process...")
        run_scraper()
        logging.info("🎉 Scraping process completed successfully.")

    except Exception as e:
        # Log any errors that occur during the process
        logging.error(f"An error occurred during the main execution: {e}", exc_info=True)
