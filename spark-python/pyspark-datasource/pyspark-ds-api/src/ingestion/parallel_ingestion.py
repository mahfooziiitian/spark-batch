from concurrent.futures import ThreadPoolExecutor

import requests


def fetch_page(page_url, headers):
    return requests.get(page_url, headers=headers).json().get("results", [])


urls = [f"https://api.example.com/data?page={i}" for i in range(1, 11)]

with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(fetch_page, urls))

flat_data = [item for sublist in results for item in sublist]
