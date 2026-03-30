#!/usr/bin/env python3
"""
Manual push: Convert XLSX to CSV, add Created Date (26 Mar 2026), push to Zoho Analytics.
"""

import os
import csv
import json
import logging
import sys
import requests
import openpyxl
from dotenv import load_dotenv

load_dotenv()

ZOHO_ORG_ID = os.getenv("ZOHO_ORG_ID")
ZOHO_WORKSPACE_ID = os.getenv("ZOHO_WORKSPACE_ID")
ZOHO_ANALYTICS_VIEW_ID = os.getenv("ZOHO_ANALYTICS_VIEW_ID")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

REQUIRED_ENV = [
    "ZOHO_CLIENT_ID", "ZOHO_CLIENT_SECRET", "ZOHO_REFRESH_TOKEN",
    "ZOHO_WORKSPACE_ID", "ZOHO_ANALYTICS_VIEW_ID", "ZOHO_ORG_ID",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("manual_push")

XLSX_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "downloads",
    "settlements - 21 Mar 26 - 26 Mar 26.xlsx",
)
CSV_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "downloads",
    "settlements_21_26_mar.csv",
)
CREATED_DATE = "26/03/2026 00:00"


def validate_env():
    missing = [var for var in REQUIRED_ENV if not os.getenv(var)]
    if missing:
        log.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)


def xlsx_to_csv():
    log.info(f"Reading XLSX: {XLSX_PATH}")
    wb = openpyxl.load_workbook(XLSX_PATH, read_only=True)
    ws = wb.active

    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            values = []
            for v in row:
                if v is None:
                    values.append("")
                elif isinstance(v, float) and v == int(v) and abs(v) > 1e15:
                    # Large floats (like ARN numbers) — write as plain integer string
                    values.append(str(int(v)))
                else:
                    values.append(v)
            if i == 0:
                values.append("Created Date")
            else:
                values.append(CREATED_DATE)
            writer.writerow(values)

    wb.close()
    total_rows = i  # last index = total data rows
    log.info(f"Converted to CSV: {CSV_PATH} ({total_rows} data rows, Created Date = {CREATED_DATE})")
    return CSV_PATH


def get_access_token():
    resp = requests.post(
        "https://accounts.zoho.in/oauth/v2/token",
        data={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"Token exchange failed: {resp.json()}")
    return token


def push_to_analytics(csv_path):
    token = get_access_token()
    url = (
        f"https://analyticsapi.zoho.in/restapi/v2/workspaces/"
        f"{ZOHO_WORKSPACE_ID}/views/{ZOHO_ANALYTICS_VIEW_ID}/data"
    )

    filename = os.path.basename(csv_path)
    log.info(f"Uploading {filename} to Zoho Analytics...")

    with open(csv_path, "rb") as f:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Zoho-oauthtoken {token}",
                "ZANALYTICS-ORGID": ZOHO_ORG_ID,
            },
            files={"FILE": (filename, f, "text/csv")},
            data={"CONFIG": json.dumps({
                "importType": "APPEND",
                "fileType": "csv",
                "autoIdentify": True,
                "onError": "SETCOLUMNEMPTY",
            })},
            timeout=300,
        )

    if resp.status_code == 200:
        summary = resp.json().get("data", {}).get("importSummary", {})
        log.info(
            f"Upload OK — added: {summary.get('successRowCount', 0)}, "
            f"failed: {summary.get('failureRowCount', 0)}, "
            f"total: {summary.get('totalRowCount', 0)}"
        )
    else:
        log.error(f"Upload failed ({resp.status_code}): {resp.text}")
        resp.raise_for_status()


def main():
    validate_env()
    csv_path = xlsx_to_csv()
    push_to_analytics(csv_path)
    os.remove(csv_path)
    log.info("Done. Temp CSV cleaned up.")


if __name__ == "__main__":
    main()
