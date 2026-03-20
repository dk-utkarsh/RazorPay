#!/usr/bin/env python3
"""
Razorpay Settlement CSV Pipeline

Downloads daily settlement CSV from Zoho Mail (sent to razorpay@dentalkart.com),
adds a 'Created Date' column, and pushes it to Zoho Analytics.

Scheduled via cron: 5 8 * * * (daily at 8:05 AM)
"""

import imaplib
import email as email_lib
import os
import csv
import json
import time
import logging
import sys
import requests
from datetime import date, datetime
from dotenv import load_dotenv

# ============ INIT ============
load_dotenv()

ZOHO_EMAIL = os.getenv("ZOHO_EMAIL")
ZOHO_APP_PASSWORD = os.getenv("ZOHO_APP_PASSWORD")
ZOHO_ORG_ID = os.getenv("ZOHO_ORG_ID")
ZOHO_WORKSPACE_ID = os.getenv("ZOHO_WORKSPACE_ID")
ZOHO_ANALYTICS_VIEW_ID = os.getenv("ZOHO_ANALYTICS_VIEW_ID")
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

IMAP_HOST = "imappro.zoho.in"
IMAP_PORT = 993
TO_FILTER = "razorpay@dentalkart.com"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")
RETRY_ATTEMPTS = 3
RETRY_DELAY = 300  # 5 minutes

REQUIRED_ENV = [
    "ZOHO_EMAIL", "ZOHO_APP_PASSWORD", "ZOHO_CLIENT_ID",
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


# ============ ZOHO MAIL ============
def fetch_csv_from_mail():
    """Download CSV attachments from today's unread emails sent to razorpay@dentalkart.com."""
    log.info("Connecting to Zoho Mail...")
    mail = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    try:
        mail.login(ZOHO_EMAIL, ZOHO_APP_PASSWORD)
        mail.select("INBOX")

        today = date.today().strftime("%d-%b-%Y")
        _, msg_ids = mail.search(None, f'(TO "{TO_FILTER}" SINCE "{today}" UNSEEN)')

        raw_ids = msg_ids[0].split()
        if not raw_ids:
            log.info("No new emails found.")
            return []

        log.info(f"Found {len(raw_ids)} unread email(s).")
        csv_files = []

        for msg_id in raw_ids:
            _, msg_data = mail.fetch(msg_id, "(RFC822)")
            msg = email_lib.message_from_bytes(msg_data[0][1])
            subject = msg.get("Subject", "(No Subject)")
            log.info(f"Processing: {subject}")

            for part in msg.walk():
                filename = part.get_filename()
                if not filename or not filename.lower().endswith(".csv"):
                    continue

                os.makedirs(DOWNLOAD_DIR, exist_ok=True)
                csv_path = os.path.join(
                    DOWNLOAD_DIR, f"{date.today().isoformat()}_{filename}"
                )
                with open(csv_path, "wb") as f:
                    f.write(part.get_payload(decode=True))

                csv_files.append(csv_path)
                log.info(f"Downloaded: {csv_path}")

            mail.store(msg_id, "+FLAGS", "\\Seen")

        return csv_files
    finally:
        mail.logout()


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
        log.info(f"Attempt {attempt}/{RETRY_ATTEMPTS}: checking mail...")
        csv_files = fetch_csv_from_mail()
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
