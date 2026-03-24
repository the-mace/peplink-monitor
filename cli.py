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

LED_LABEL = {
    "green": "connected",
    "red": "disconnected",
    "yellow": "degraded",
    "orange": "degraded",
    "empty": "no device",
    "gray": "disabled",
}


def _led_label(status_led: str) -> str:
    return LED_LABEL.get(status_led, status_led or "unknown")


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

    pings = db.get_latest_wan_ping_all(conn)
    if pings:
        print()
        ping_rows = [
            [p["isp"].capitalize(), f"{p['ping_ms']:.1f} ms", fmt_age(p["timestamp"])]
            for p in pings
        ]
        print(tabulate(ping_rows, headers=["ISP", "Ping (8.8.8.8)", "Sampled"], tablefmt="simple"))


def cmd_summary(conn, period: str, wan_filter: str | None, show_all: bool = False) -> None:
    seconds = PERIODS[period]
    now = int(time.time())
    start_ts = now - seconds

    rows_tp = db.get_throughput_in_period(conn, start_ts, now)

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

    # Count failover events (green→non-green transitions) in period from API health data
    failover_counts = db.count_health_failovers_in_period(conn, start_ts, now)

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

    ping_rows_raw = db.get_wan_ping_in_period(conn, start_ts, now)
    if ping_rows_raw:
        by_isp: dict[str, list[float]] = defaultdict(list)
        for p in ping_rows_raw:
            by_isp[p["isp"]].append(p["ping_ms"])
        ping_summary = [
            [
                isp.capitalize(),
                len(vals),
                f"{min(vals):.1f} ms",
                f"{sum(vals)/len(vals):.1f} ms",
                f"{max(vals):.1f} ms",
            ]
            for isp, vals in sorted(by_isp.items())
        ]
        print()
        print(tabulate(
            ping_summary,
            headers=["ISP", "Samples", "Min Ping", "Avg Ping", "Max Ping"],
            tablefmt="simple",
        ))


def _derive_health_events(raw_events: list[dict]) -> list[dict]:
    """
    Annotate raw health_events rows with a human-readable event type and
    outage duration for recovery events.

    Each output dict: {wan_name, event, from_status, to_status, timestamp,
                       message, duration_seconds}
    """
    by_wan: dict[str, list[dict]] = defaultdict(list)
    for e in raw_events:
        by_wan[e["wan_name"]].append(e)

    result = []
    for wan_name, events in by_wan.items():
        events.sort(key=lambda x: x["timestamp"])
        down_at: int | None = None
        for e in events:
            old_led = e["old_status"]
            new_led = e["new_status"]
            if old_led == "green" and new_led != "green":
                event_type = "went down"
                down_at = e["timestamp"]
                duration = None
            elif old_led != "green" and new_led == "green":
                event_type = "came up"
                duration = (e["timestamp"] - down_at) if down_at is not None else None
                down_at = None
            else:
                event_type = "status changed"
                duration = None
            result.append({
                "wan_name": wan_name,
                "event": event_type,
                "from_status": _led_label(old_led),
                "to_status": _led_label(new_led),
                "timestamp": e["timestamp"],
                "message": e["message"],
                "duration_seconds": duration,
            })

    result.sort(key=lambda x: x["timestamp"])
    return result


def cmd_failovers(conn, wan_filter: str | None, show_all: bool = False) -> None:
    raw_events = db.get_health_events(conn, wan_filter)
    events = _derive_health_events(raw_events)

    if not events:
        print("No failover events recorded.")
        print("(Health event tracking starts when the Peplink API is configured.)")
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
            e["wan_name"],
            e["event"],
            e["from_status"],
            e["to_status"],
            e["message"],
            fmt_ts(e["timestamp"]),
            duration,
        ])

    print(tabulate(
        rows,
        headers=["WAN", "Event", "From", "To", "Message", "Timestamp", "Duration"],
        tablefmt="simple",
    ))



def cmd_daily(conn, days: int, wan_filter: str | None, show_all: bool = False) -> None:
    now = int(time.time())
    start_ts = now - days * 86400

    rows_tp = db.get_throughput_daily(conn, start_ts, now)

    if wan_filter:
        rows_tp = [r for r in rows_tp if r["name"] == wan_filter]

    if not show_all:
        ever_up = db.get_interfaces_ever_up(conn)
        rows_tp = [r for r in rows_tp if r["interface_id"] in ever_up]

    if not rows_tp:
        print(f"No throughput data in the last {days} day(s).")
        return

    failover_counts = db.count_health_failovers_daily(conn, start_ts, now)

    table_rows = []
    for r in rows_tp:
        failovers = failover_counts.get((r["day"], r["name"]), 0)
        table_rows.append([
            r["day"],
            r["name"],
            fmt_mbps(r["peak_in"]),
            fmt_mbps(r["peak_out"]),
            fmt_mbps(r["avg_in"]),
            fmt_mbps(r["avg_out"]),
            fmt_bytes(r["total_in"]),
            fmt_bytes(r["total_out"]),
            failovers,
        ])

    print(tabulate(
        table_rows,
        headers=[
            "Date", "Interface", "Peak In", "Peak Out",
            "Avg In", "Avg Out",
            "Total In", "Total Out", "Failovers",
        ],
        tablefmt="simple",
    ))

    ping_rows_raw = db.get_wan_ping_daily(conn, start_ts, now)
    if ping_rows_raw:
        print()
        ping_table = [
            [
                p["day"],
                p["isp"].capitalize(),
                f"{p['min_ping']:.1f} ms",
                f"{p['avg_ping']:.1f} ms",
                f"{p['max_ping']:.1f} ms",
            ]
            for p in ping_rows_raw
        ]
        print(tabulate(
            ping_table,
            headers=["Date", "ISP", "Min Ping", "Avg Ping", "Max Ping"],
            tablefmt="simple",
        ))


def cmd_ping(conn, period: str) -> None:
    seconds = PERIODS[period]
    now = int(time.time())
    start_ts = now - seconds

    rows_raw = db.get_wan_ping_in_period(conn, start_ts, now)
    if not rows_raw:
        print(f"No ping data in the last {period}.")
        return

    print(f"WAN ping history — last {period}\n")
    rows = [
        [fmt_ts(p["timestamp"]), p["isp"].capitalize(), f"{p['ping_ms']:.1f} ms"]
        for p in reversed(rows_raw)
    ]
    print(tabulate(rows, headers=["Timestamp", "ISP", "Ping (8.8.8.8)"], tablefmt="simple"))


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

    daily_p = subs.add_parser("daily", help="Per-day per-WAN throughput and ping summary")
    daily_p.add_argument(
        "--days",
        type=int,
        default=7,
        metavar="N",
        help="Number of days to show (default: 7)",
    )

    ping_p = subs.add_parser("ping", help="WAN ping latency history")
    ping_p.add_argument(
        "--period",
        choices=list(PERIODS),
        default="24h",
        help="Time period (default: 24h)",
    )

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
    remote_python = cfg.get("remote_python", "python3")
    # Rebuild argv without --remote, pass everything else through
    remote_args = [a for a in sys.argv[1:] if a != "--remote"]
    cmd = " ".join(shlex.quote(a) for a in [remote_python, remote_script] + remote_args)
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
        elif args.command == "daily":
            cmd_daily(conn, args.days, args.wan, show_all=args.show_all)
        elif args.command == "ping":
            cmd_ping(conn, args.period)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
