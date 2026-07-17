#!/usr/bin/env python3
"""Rolls up raw throughput/latency samples into daily aggregates.

Designed to be run once daily via cron, independent of collector.py's
5-minute poll. Re-rolls up yesterday and today on every run: yesterday as a
cheap idempotent correctness check, today so a same-day report always sees
current data without waiting for the day to close out.

Optional raw retention: if config ``raw_retention_days`` > 0, deletes raw
readings/throughput/wan_latency older than that many days after rollup
(rollups and health_events are kept).
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone

import db
from config import load_config


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

        retention = int(cfg.get("raw_retention_days") or 0)
        if retention > 0:
            cutoff = int(
                datetime.combine(
                    today - timedelta(days=retention),
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                ).timestamp()
            )
            deleted = db.prune_raw_samples(conn, cutoff)
            print(
                f"Pruned raw samples older than {retention}d "
                f"(readings={deleted['readings']}, throughput={deleted['throughput']}, "
                f"wan_latency={deleted['wan_latency']})"
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
