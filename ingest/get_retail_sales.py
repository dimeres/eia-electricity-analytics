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


BASE_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"


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
            "data[]": ["customers", "price", "revenue", "sales"],
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


def ingest_retail_sales():
    records = fetch_all_pages()

    raw_path = RAW_DIR / f"retail_sales_raw_{datetime.today().strftime('%Y%m%d')}.json"
    with raw_path.open("w") as f:
        json.dump(records, f, indent=2)

    df = pd.DataFrame(records)
    df["period"] = pd.to_datetime(df["period"])

    # Convert raw records into a tabular structure
    clean_path = CLEAN_DIR / "retail_sales_clean.csv"
    df.to_csv(clean_path, index=False)

    print(f"Downloaded {len(df)} rows.")
    print(f"Raw JSON saved → {raw_path}")
    print(f"Clean CSV saved → {clean_path}")


if __name__ == "__main__":
    ingest_retail_sales()
