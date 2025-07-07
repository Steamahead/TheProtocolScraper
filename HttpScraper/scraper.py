import requests
from bs4 import BeautifulSoup
from typing import List, Dict

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

URL = (
    "https://theprotocol.it/filtry/big-data-science;"
    "sp/junior,assistant,trainee,mid;"
    "p/warszawa;wp/praca/bi-developer-warszawa-zubra-1,oferta,"  
    "952b0000-9568-9a92-a6a4-08ddba1ba907"
)


def fetch_listings() -> List[Dict[str, str]]:
    """
    Fetches the page and returns a list of dicts with 'title', 'company', and 'link'.
    """
    resp = requests.get(URL, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'html.parser')
    jobs = soup.select('div.offer-card')
    results = []

    for job in jobs:
        title_elem = job.select_one('h2.title')
        company_elem = job.select_one('div.company')
        link_elem = job.select_one('a.offer-link')

        if not title_elem or not company_elem or not link_elem:
            continue

        results.append({
            'title': title_elem.get_text(strip=True),
            'company': company_elem.get_text(strip=True),
            'link': link_elem['href']
        })

    return results
