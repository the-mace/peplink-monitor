#!/usr/bin/env python3
"""Rolls up raw throughput/latency samples into daily aggregates.

Designed to be run once daily via cron, independent of collector.py's
5-minute poll. Re-rolls up yesterday and today on every run: yesterday as a
cheap idempotent correctness check, today so a same-day report always sees
current data without waiting for the day to close out.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

import db


PROJECT_DIR = Path(__file__).parent


def load_config() -> dict:
    with open(PROJECT_DIR / "config.yaml") as fh:
        cfg = yaml.safe_load(fh)
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_DIR / db_path
    cfg["db_path"] = str(db_path)
    return cfg


def main() -> None:
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    cfg = load_config()
    conn = db.get_connection(cfg["db_path"])
    db.init_db(conn)

    today = datetime.now(timezone.utc).date()
    start_day = today - timedelta(days=days - 1)
    start_ts = int(datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    end_ts = int(time.time())

    try:
        db.rollup_range(conn, start_ts, end_ts)
        print(f"Rolled up throughput_daily / latency_daily for {start_day} .. {today} UTC")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
