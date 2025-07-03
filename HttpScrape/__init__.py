import logging
import azure.functions as func
import requests
from bs4 import BeautifulSoup


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing scrape request...')

    # 1. URL to fetch
    url = (
        "https://theprotocol.it/filtry/big-data-science;"
        "sp/junior,assistant,trainee,mid;"
        "p/warszawa;wp/praca/bi-developer-warszawa-zubra-1,oferta,"  
        "952b0000-9568-9a92-a6a4-08ddba1ba907"
    )

    # 2. Set headers to mimic a browser
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    # 3. Fetch page with headers
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Fetch error: {e}")
        return func.HttpResponse(f"Error fetching page: {e}", status_code=500)

    # 4. Parse title as a quick check
    soup = BeautifulSoup(resp.text, 'html.parser')
    title = soup.title.string if soup.title else 'No title found'

    # 5. Return result
    return func.HttpResponse(f"Fetched successfully! Title: {title}")