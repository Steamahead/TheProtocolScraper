import os
import pyodbc
from models import JobListing

# Connection string; configure via Application Settings in Azure Portal
DB_CONN_STR = os.getenv('DB_CONN_STR')

# Example using ODBC Driver
# Ensure you have added 'pyodbc' to requirements.txt

def get_connection():
    if not DB_CONN_STR:
        raise ValueError("Database connection string not found in DB_CONN_STR environment variable.")
    return pyodbc.connect(DB_CONN_STR)


def insert_job_listing(listing: JobListing) -> int:
    """
    Inserts a JobListing into the database and returns the new record's ID.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Adjust table/column names to your schema
    query = (
        "INSERT INTO JobListings "
        "(JobId, Source, Title, Company, Link, ScrapeDate, ListingStatus) "
        "OUTPUT INSERTED.ShortId "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    params = (
        listing.job_id,
        listing.source,
        listing.title,
        listing.company,
        listing.link,
        listing.scrape_date,
        listing.listing_status
    )

    cursor.execute(query, params)
    short_id = cursor.fetchval()  # fetch the OUTPUT value
    conn.commit()
    cursor.close()
    conn.close()
    return short_id
