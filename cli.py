#!/usr/bin/env python3
"""CLI query tool for peplink-monitor data."""

import argparse
import shlex
import socket
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import yaml
from tabulate import tabulate

import db


PERIODS = {
    "1h": 3600,
    "24h": 86400,
    "7d": 7 * 86400,
    "30d": 30 * 86400,
}

STATUS_LABEL = {1: "up", 2: "down"}


PROJECT_DIR = Path(__file__).parent


def load_config() -> dict:
    with open(PROJECT_DIR / "config.yaml") as fh:
        cfg = yaml.safe_load(fh)
    db_path = Path(cfg["db_path"])
    if not db_path.is_absolute():
        db_path = PROJECT_DIR / db_path
    cfg["db_path"] = str(db_path)
    return cfg


def fmt_mbps(mbps: float) -> str:
    return f"{mbps:.2f} Mbps"


def fmt_bytes(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f} GB"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f} MB"
    return f"{n / 1_000:.2f} KB"


def fmt_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_age(ts: int) -> str:
    age = int(time.time()) - ts
    if age < 60:
        return f"{age}s ago"
    if age < 3600:
        return f"{age // 60}m ago"
    return f"{age // 3600}h {(age % 3600) // 60}m ago"


def fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def cmd_current(conn, wan_filter: str | None, show_all: bool = False) -> None:
    readings = db.get_latest_readings_all(conn)
    throughputs = {t["interface_id"]: t for t in db.get_latest_throughput_all(conn)}
    ever_up = db.get_interfaces_ever_up(conn)

    if wan_filter:
        readings = [r for r in readings if r["name"] == wan_filter]
        if not readings:
            print(f"No data found for WAN: {wan_filter}")
            sys.exit(1)

    if not readings:
        print("No readings in database. Has the collector run yet?")
        sys.exit(1)

    # Interfaces that have never been up are hidden by default
    if not show_all:
        readings = [r for r in readings if r["interface_id"] in ever_up]

    if not readings:
        print("No active interfaces. Use --show-all to include interfaces that have never been up.")
        sys.exit(0)

    rows = []
    for r in readings:
        tp = throughputs.get(r["interface_id"])
        mbps_in = fmt_mbps(tp["mbps_in"]) if tp else "—"
        mbps_out = fmt_mbps(tp["mbps_out"]) if tp else "—"
        rows.append([
            r["name"],
            r["label"],
            STATUS_LABEL.get(r["oper_status"], "unknown"),
            mbps_in,
            mbps_out,
            fmt_age(r["timestamp"]),
        ])

    print(tabulate(
        rows,
        headers=["Interface", "Label", "Status", "In", "Out", "Last Poll"],
        tablefmt="simple",
    ))


def cmd_summary(conn, period: str, wan_filter: str | None, show_all: bool = False) -> None:
    seconds = PERIODS[period]
    now = int(time.time())
    start_ts = now - seconds

    rows_tp = db.get_throughput_in_period(conn, start_ts, now)
    rows_rd = db.get_readings_for_failovers(conn, wan_filter)

    if wan_filter:
        rows_tp = [r for r in rows_tp if r["name"] == wan_filter]

    if not show_all:
        ever_up = db.get_interfaces_ever_up(conn)
        rows_tp = [r for r in rows_tp if r["interface_id"] in ever_up]

    if not rows_tp:
        print(f"No throughput data in the last {period}.")
        sys.exit(0)

    # Group throughput by interface name
    by_iface: dict[str, list[dict]] = defaultdict(list)
    for row in rows_tp:
        by_iface[row["name"]].append(row)

    # Count failover events (up→down transitions) in period
    failover_counts = _count_failovers_in_period(rows_rd, start_ts, wan_filter)

    table_rows = []
    for name, records in by_iface.items():
        label = records[0]["label"]
        peak_in = max(r["mbps_in"] for r in records)
        peak_out = max(r["mbps_out"] for r in records)
        avg_in = sum(r["mbps_in"] for r in records) / len(records)
        avg_out = sum(r["mbps_out"] for r in records) / len(records)
        total_in = sum(r["delta_bytes_in"] for r in records)
        total_out = sum(r["delta_bytes_out"] for r in records)
        failovers = failover_counts.get(name, 0)
        table_rows.append([
            name,
            label,
            fmt_mbps(peak_in),
            fmt_mbps(peak_out),
            fmt_mbps(avg_in),
            fmt_mbps(avg_out),
            fmt_bytes(total_in),
            fmt_bytes(total_out),
            failovers,
        ])

    print(f"Summary — last {period}\n")
    print(tabulate(
        table_rows,
        headers=[
            "Interface", "Label", "Peak In", "Peak Out",
            "Avg In", "Avg Out",
            "Total In", "Total Out", "Failovers",
        ],
        tablefmt="simple",
    ))


def _count_failovers_in_period(
    readings: list[dict],
    start_ts: int,
    wan_filter: str | None,
) -> dict[str, int]:
    """Count up→down transitions per interface within the period."""
    by_iface: dict[str, list[dict]] = defaultdict(list)
    for r in readings:
        by_iface[r["name"]].append(r)

    counts: dict[str, int] = {}
    for name, records in by_iface.items():
        records.sort(key=lambda x: x["timestamp"])
        count = 0
        for i in range(1, len(records)):
            if records[i]["timestamp"] < start_ts:
                continue
            if records[i - 1]["oper_status"] == 1 and records[i]["oper_status"] != 1:
                count += 1
        counts[name] = count
    return counts


def _derive_failover_events(readings: list[dict]) -> list[dict]:
    """
    Convert a flat list of readings into a list of state-change events.
    Each event: {name, event, timestamp, duration_seconds}
    """
    by_iface: dict[str, list[dict]] = defaultdict(list)
    for r in readings:
        by_iface[r["name"]].append(r)

    events = []
    for name, records in by_iface.items():
        records.sort(key=lambda x: x["timestamp"])
        prev_status = None
        down_at = None

        label = records[0]["label"]
        for r in records:
            status = r["oper_status"]
            if prev_status is None:
                prev_status = status
                continue

            if prev_status == 1 and status != 1:
                # Interface went down
                down_at = r["timestamp"]
                events.append({
                    "name": name,
                    "label": label,
                    "event": "went down",
                    "timestamp": r["timestamp"],
                    "duration_seconds": None,
                })
            elif prev_status != 1 and status == 1:
                # Interface came back up
                duration = (r["timestamp"] - down_at) if down_at is not None else None
                events.append({
                    "name": name,
                    "label": label,
                    "event": "came up",
                    "timestamp": r["timestamp"],
                    "duration_seconds": duration,
                })
                down_at = None

            prev_status = status

    events.sort(key=lambda x: x["timestamp"])
    return events


def cmd_failovers(conn, wan_filter: str | None, show_all: bool = False) -> None:
    if not show_all:
        ever_up = db.get_interfaces_ever_up(conn)
    readings = db.get_readings_for_failovers(conn, wan_filter)
    if not show_all:
        readings = [r for r in readings if r["interface_id"] in ever_up]
    events = _derive_failover_events(readings)

    if not events:
        print("No failover events recorded.")
        return

    rows = []
    for e in events:
        if e["duration_seconds"] is not None:
            duration = fmt_duration(e["duration_seconds"])
        elif e["event"] == "went down":
            duration = "ongoing"
        else:
            duration = "—"
        rows.append([
            e["name"],
            e["label"],
            e["event"],
            fmt_ts(e["timestamp"]),
            duration,
        ])

    print(tabulate(
        rows,
        headers=["Interface", "Label", "Event", "Timestamp", "Duration"],
        tablefmt="simple",
    ))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Query peplink-monitor data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Run against the remote host configured in config.yaml",
    )
    parser.add_argument(
        "--wan",
        metavar="NAME",
        help="Filter output to a specific WAN interface by name",
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Include interfaces that have never been up",
    )

    subs = parser.add_subparsers(dest="command", required=True)

    subs.add_parser("current", help="Latest reading for all interfaces")

    summary_p = subs.add_parser("summary", help="Throughput statistics for a time period")
    summary_p.add_argument(
        "--period",
        choices=list(PERIODS),
        default="24h",
        help="Time period (default: 24h)",
    )

    subs.add_parser("failovers", help="Chronological list of all interface state changes")

    return parser


def _local_ips() -> set[str]:
    """Return all IP addresses assigned to this machine."""
    ips = {"127.0.0.1", "::1"}
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            ips.add(info[4][0])
    except socket.gaierror:
        pass
    return ips


def _is_local(host: str) -> bool:
    """Return True if host resolves to this machine."""
    try:
        remote_ip = socket.gethostbyname(host)
    except socket.gaierror:
        return False
    return remote_ip in _local_ips()


def run_remote(cfg: dict) -> None:
    host = cfg.get("remote_host")
    user = cfg.get("remote_user", "rob")
    if not host:
        print("Error: remote_host not set in config.yaml", file=sys.stderr)
        sys.exit(1)

    if _is_local(host):
        return  # Remote points at this machine — run locally

    remote_script = str(PROJECT_DIR / "cli.py")
    # Rebuild argv without --remote, pass everything else through
    remote_args = [a for a in sys.argv[1:] if a != "--remote"]
    cmd = " ".join(shlex.quote(a) for a in [remote_script] + remote_args)
    result = subprocess.run(["ssh", "-A", f"{user}@{host}", cmd])
    sys.exit(result.returncode)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    cfg = load_config()

    if args.remote:
        run_remote(cfg)

    conn = db.get_connection(cfg["db_path"])
    db.init_db(conn)

    try:
        if args.command == "current":
            cmd_current(conn, args.wan, show_all=args.show_all)
        elif args.command == "summary":
            cmd_summary(conn, args.period, args.wan, show_all=args.show_all)
        elif args.command == "failovers":
            cmd_failovers(conn, args.wan, show_all=args.show_all)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
