import logging
import azure.functions as func
import json
from scraper import fetch_listings
from models import JobListing
from database import insert_job_listing, create_tables_if_not_exist

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HttpScrape function started.')

    try:
        # 1. Create database tables if they don't already exist
        logging.info("Ensuring database tables exist...")
        create_tables_if_not_exist()
        logging.info("Database tables are ready.")

        # 2. Fetch job listings from the scraper
        logging.info("Fetching job listings...")
        scraped_jobs = fetch_listings()
        logging.info(f"Found {len(scraped_jobs)} jobs.")

        # 3. Insert or update jobs in the database
        inserted_count = 0
        for job_data in scraped_jobs:
            # Create a JobListing object from the scraped data
            job = JobListing(
                job_id=job_data.get('link'),  # Using the link as a unique ID
                source='theprotocol.it',
                title=job_data.get('title'),
                company=job_data.get('company'),
                link=job_data.get('link')
            )
            
            # Insert the job listing into the database
            inserted_id = insert_job_listing(job)
            if inserted_id:
                inserted_count += 1
                logging.info(f"Successfully inserted job with ID: {inserted_id}")

        return func.HttpResponse(
            body=json.dumps({
                'status': 'success',
                'scraped_jobs': len(scraped_jobs),
                'newly_inserted': inserted_count
            }),
            status_code=200,
            mimetype='application/json'
        )

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)
