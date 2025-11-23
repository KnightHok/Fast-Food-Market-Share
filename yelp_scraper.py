import os
import json

import requests
from dotenv import load_dotenv

def get_actors():
    res = requests.get("https://api.apify.com/v2/acts", headers={
        'Accept': 'application/json',
        'Authorization': f'Bearer {os.getenv("APIFY_TOKEN")}'
    })

    print(json.dumps(res.json(), indent=4))

def run_yelp_scraper_actor():
    payload = {
        "locations": [
            "Tampa, FL",
            "Miami, FL"
        ],
        "maxImages": 0,
        "searchLimit": 15,
        "searchTerms": [
            "Fast Food",
            "McDonald's"
        ]
    }
    res = requests.post("https://api.apify.com/v2/acts/bf54TfrKoJrQZsrZm/runs", headers={
        'Accept': 'application/json',
        'Authorization': f'Bearer {os.getenv("APIFY_TOKEN")}'
    },
    json=payload)

    print(json.dumps(res.json(), indent=4))

if __name__ == "__main__":
    load_dotenv(".env")
    # print(os.getenv("APIFY_TOKEN"))
    run_yelp_scraper_actor()