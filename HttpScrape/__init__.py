import logging
import sys
import os

# --- DETAILED DIAGNOSTICS FOR IMPORT ERRORS ---
# This section will log the environment details to help us debug.
logging.info(f"--- Python version: {sys.version}")
logging.info(f"--- sys.path before modification: {sys.path}")
logging.info(f"--- Current working directory: {os.getcwd()}")

# Add the project root to the system path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
logging.info(f"--- Project root added to path: {project_root}")
logging.info(f"--- sys.path after modification: {sys.path}")

# --- Import modules within a try/except block to catch the specific error ---
try:
    import azure.functions as func
    import json
    from scraper import run_scraper
    from database import create_tables_if_not_exist
    logging.info("--- All local modules imported successfully. ---")
except ImportError as e:
    logging.error(f"--- FAILED TO IMPORT MODULE: {e.name}, PATH: {e.path} ---")
    # Log all files in the project root to see if something is missing
    try:
        files_in_root = os.listdir(project_root)
        logging.error(f"--- Files in project root ('{project_root}'): {files_in_root} ---")
    except Exception as list_e:
        logging.error(f"--- Could not list files in project root: {list_e} ---")
    # Re-raise the exception to ensure the function fails as expected, but after logging.
    raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function main execution started.')

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
