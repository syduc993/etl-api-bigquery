"""
Trigger webview messenger service to send store-based messcard and matrix report.
Uses Cloud Run service base URL from env MESSCARD_BASE_URL.
"""
import os
import sys
import time
import logging
from typing import Tuple

import requests

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


BASE_URL = os.getenv(
    "MESSCARD_BASE_URL",
    "https://webview-messenger-reader-858039461446.asia-southeast1.run.app",
).rstrip("/")

ENDPOINTS = [
    "/api/cron/send-report",
    "/api/cron/send-matrix-report",
]

RETRIES = int(os.getenv("RETRIES", "3"))
BACKOFF_SECONDS = float(os.getenv("BACKOFF_SECONDS", "5"))
TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "60"))


def call_endpoint(path: str) -> Tuple[bool, str]:
    url = f"{BASE_URL}{path}"
    for attempt in range(1, RETRIES + 1):
        try:
            logger.info("POST %s (attempt %s/%s)", url, attempt, RETRIES)
            resp = requests.post(
                url,
                headers={"Content-Type": "application/json"},
                timeout=TIMEOUT,
            )
            body = resp.text
            if resp.status_code >= 200 and resp.status_code < 300:
                logger.info("Success %s: status=%s body=%s", path, resp.status_code, body)
                return True, body
            logger.warning(
                "Non-2xx %s: status=%s body=%s", path, resp.status_code, body
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Error calling %s: %s", url, exc)
        if attempt < RETRIES:
            time.sleep(BACKOFF_SECONDS * attempt)
    return False, f"Failed after {RETRIES} attempts"


def main() -> int:
    all_ok = True
    for ep in ENDPOINTS:
        ok, msg = call_endpoint(ep)
        if not ok:
            logger.error("Endpoint %s failed: %s", ep, msg)
            all_ok = False
    if not all_ok:
        return 1
    logger.info("All messcard endpoints executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

