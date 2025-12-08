import os
import json
import pathlib
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")
if not API_KEY:
    raise RuntimeError("EIA_API_KEY not found")

# Define project directories relative to repo root
ROOT = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data_raw"
CLEAN_DIR = ROOT / "data_clean"

RAW_DIR.mkdir(exist_ok=True)
CLEAN_DIR.mkdir(exist_ok=True)


BASE_URL = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"


# 50 states + DC
STATE_IDS = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC"
]


def fetch_all_pages(start="2010-01", end=None):
    """
    Pagination-enabled fetch from EIA API.
    Returns full dataset from 2010 to present.
    """
    if end is None:
        end = datetime.today().strftime("%Y-%m")

    all_records = []
    offset = 0
    page_size = 5000

    while True:
        params = {
            "api_key": API_KEY,
            "frequency": "monthly",
            "data[]": ["consumption-for-eg", "consumption-for-eg-btu", "cost", "generation"],
            "facets[sectorid][]": "99",
            "start": start,
            "end": end,
            "offset": offset,
            "length": page_size,
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
        }

        r = requests.get(BASE_URL, params=params)
        r.raise_for_status()
        data = r.json()

        records = data["response"]["data"]
        if len(records) == 0:
            break

        all_records.extend(records)
        offset += page_size

    return all_records


def ingest_generation():
    records = fetch_all_pages()

    raw_path = RAW_DIR / f"generation_raw_{datetime.today().strftime('%Y%m%d')}.json"
    with raw_path.open("w") as f:
        json.dump(records, f, indent=2)

    df = pd.DataFrame(records)
    df["period"] = pd.to_datetime(df["period"], errors="raise")

    df = df[df["location"].isin(STATE_IDS)]

    # Convert raw records into a tabular structure
    clean_path = CLEAN_DIR / "generation_clean.csv"
    df.to_csv(clean_path, index=False)

    print(f"Downloaded Total Raw {len(records)} rows.")
    print(f"Rows after filtering to states: {len(df)}")
    print(f"Raw JSON saved → {raw_path}")
    print(f"Clean CSV saved → {clean_path}")


if __name__ == "__main__":
    ingest_generation()
