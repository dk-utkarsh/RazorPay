#!/usr/bin/env python3
"""
Razorpay Settlement CSV Pipeline

Downloads daily settlement CSV via Razorpay API,
adds a 'Created Date' column, and pushes it to Zoho Analytics.

Scheduled via cron: 5 8 * * * (daily at 8:05 AM)
"""

import os
import csv
import json
import time
import logging
import sys
import requests
from requests.auth import HTTPBasicAuth
from datetime import date, datetime
from dotenv import load_dotenv

# ============ INIT ============
load_dotenv()

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET")
ZOHO_ORG_ID = os.getenv("ZOHO_ORG_ID")
ZOHO_WORKSPACE_ID = os.getenv("ZOHO_WORKSPACE_ID")
ZOHO_ANALYTICS_VIEW_ID = os.getenv("ZOHO_ANALYTICS_VIEW_ID")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
RETRY_ATTEMPTS = 3
RETRY_DELAY = 300  # 5 minutes

REQUIRED_ENV = [
    "RAZORPAY_KEY_ID", "RAZORPAY_KEY_SECRET", "ZOHO_CLIENT_ID",
    "ZOHO_CLIENT_SECRET", "ZOHO_REFRESH_TOKEN",
    "ZOHO_WORKSPACE_ID", "ZOHO_ANALYTICS_VIEW_ID", "ZOHO_ORG_ID",
]

# ============ LOGGING ============
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "pipeline.log")),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("razorpay_pipeline")


# ============ RAZORPAY API ============
def fetch_csv_from_razorpay():
    """Download today's settlement recon CSV via Razorpay API."""
    today = date.today()
    log.info(f"Fetching settlement report from Razorpay API for {today.isoformat()}...")

    resp = requests.get(
        "https://api.razorpay.com/v1/settlements/recon/combined",
        auth=HTTPBasicAuth(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
        params={"year": today.year, "month": today.month, "day": today.day},
        timeout=120,
    )

    if resp.status_code == 200:
        content_type = resp.headers.get("Content-Type", "")
        if "text/csv" in content_type or "application/octet-stream" in content_type or resp.content:
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            filename = f"{today.isoformat()}_settlement_recon.csv"
            csv_path = os.path.join(DOWNLOAD_DIR, filename)
            with open(csv_path, "wb") as f:
                f.write(resp.content)
            log.info(f"Downloaded: {csv_path} ({len(resp.content)} bytes)")
            return [csv_path]

    if resp.status_code == 404:
        log.info("No settlement data available for today yet.")
        return []

    log.error(f"Razorpay API error ({resp.status_code}): {resp.text}")
    resp.raise_for_status()
    return []


# ============ CSV TRANSFORM ============
def add_created_date(csv_path):
    """Append 'Created Date' column with current timestamp (dd/MM/yyyy HH:mm)."""
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")

    with open(csv_path, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    rows[0].append("Created Date")
    for row in rows[1:]:
        row.append(timestamp)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        csv.writer(f).writerows(rows)

    log.info(f"Added 'Created Date' ({timestamp}) to {len(rows) - 1} rows.")


# ============ ZOHO ANALYTICS ============
def get_access_token():
    """Exchange refresh token for a fresh access token."""
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
    """Upload CSV to Zoho Analytics via import API."""
    token = get_access_token()
    url = (
        f"https://analyticsapi.zoho.in/restapi/v2/workspaces/"
        f"{ZOHO_WORKSPACE_ID}/views/{ZOHO_ANALYTICS_VIEW_ID}/data"
    )

    filename = os.path.basename(csv_path)
    log.info(f"Uploading {filename}...")

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
            })},
            timeout=120,
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


# ============ PIPELINE ============
def validate_env():
    """Check all required environment variables are set."""
    missing = [var for var in REQUIRED_ENV if not os.getenv(var)]
    if missing:
        log.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)


def run():
    """Main pipeline: fetch CSV → add Created Date → push to analytics → cleanup."""
    log.info(f"{'=' * 40}")
    log.info(f"Pipeline started — {datetime.now()}")
    log.info(f"{'=' * 40}")

    validate_env()

    # Fetch with retries (email might arrive late)
    csv_files = []
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        log.info(f"Attempt {attempt}/{RETRY_ATTEMPTS}: fetching from Razorpay API...")
        csv_files = fetch_csv_from_razorpay()
        if csv_files:
            break
        if attempt < RETRY_ATTEMPTS:
            log.info(f"No CSV yet. Retrying in {RETRY_DELAY // 60}m...")
            time.sleep(RETRY_DELAY)

    if not csv_files:
        log.warning("No CSV found after all retries.")
        return

    # Process each file
    success = 0
    for csv_path in csv_files:
        try:
            add_created_date(csv_path)
            push_to_analytics(csv_path)
            os.remove(csv_path)
            log.info(f"Done & cleaned: {os.path.basename(csv_path)}")
            success += 1
        except Exception as e:
            log.error(f"Failed {os.path.basename(csv_path)}: {e}")

    log.info(f"Pipeline complete — {success}/{len(csv_files)} files pushed.")


if __name__ == "__main__":
    run()
