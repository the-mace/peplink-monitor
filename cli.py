#!/usr/bin/env python3
"""CLI query tool for peplink-monitor data."""

from __future__ import annotations

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
import report


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
    report_dir = Path(cfg.get("report_dir", "reports"))
    if not report_dir.is_absolute():
        report_dir = PROJECT_DIR / report_dir
    cfg["report_dir"] = str(report_dir)
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
    health_by_name = {
        v["wan_name"]: v
        for v in db.get_wan_health_states(conn).values()
    }

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
        h = health_by_name.get(r["name"])
        if h:
            status = _led_label(h["status_led"])
            message = h["message"] or "—"
            uptime = fmt_duration(h["uptime_seconds"]) if h["uptime_seconds"] else "—"
        else:
            status = STATUS_LABEL.get(r["oper_status"], "unknown")
            message = "—"
            uptime = "—"
        rows.append([
            r["name"],
            r["label"],
            status,
            message,
            mbps_in,
            mbps_out,
            uptime,
            fmt_age(r["timestamp"]),
        ])

    print(tabulate(
        rows,
        headers=["Interface", "Label", "Status", "Message", "In", "Out", "Uptime", "Last Poll"],
        tablefmt="simple",
    ))

    latencies = db.get_latest_wan_latency_all(conn)
    if latencies:
        print()
        lat_rows = [
            [
                l["wan_name"],
                f"{l['latency_min']:.1f} ms",
                f"{l['latency_avg']:.1f} ms",
                f"{l['latency_max']:.1f} ms",
                fmt_age(l["timestamp"]),
            ]
            for l in latencies
            if l["source"] == "api"
        ]
        if lat_rows:
            print(tabulate(
                lat_rows,
                headers=["WAN", "Min", "Avg", "Max", "Sampled"],
                tablefmt="simple",
            ))
            print("  (router-measured, not client ping)")


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

    latency_rows = db.get_wan_latency_in_period(conn, start_ts, now)
    if latency_rows:
        by_wan: dict[str, list] = defaultdict(list)
        for row in latency_rows:
            by_wan[row["wan_name"]].append(row)
        lat_summary = [
            [
                wan_name,
                len(rows),
                f"{min(r['latency_min'] for r in rows):.1f} ms",
                f"{sum(r['latency_avg'] for r in rows) / len(rows):.1f} ms",
                f"{max(r['latency_max'] for r in rows):.1f} ms",
            ]
            for wan_name, rows in sorted(by_wan.items())
        ]
        print()
        print(tabulate(
            lat_summary,
            headers=["WAN", "Samples", "Min", "Avg", "Max"],
            tablefmt="simple",
        ))
        print("  (router-measured, not client ping)")


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
        last_down_event: dict | None = None
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
                if last_down_event is not None and duration is not None:
                    last_down_event["duration_seconds"] = duration
                down_at = None
                last_down_event = None
            else:
                event_type = "status changed"
                duration = None
                last_down_event = None
            entry = {
                "wan_name": wan_name,
                "event": event_type,
                "from_status": _led_label(old_led),
                "to_status": _led_label(new_led),
                "timestamp": e["timestamp"],
                "message": e["message"],
                "duration_seconds": duration,
                "source": e.get("source", "poll"),
            }
            result.append(entry)
            if event_type == "went down":
                last_down_event = entry

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
            e.get("source", "poll"),
        ])

    print(tabulate(
        rows,
        headers=["WAN", "Event", "From", "To", "Message", "Timestamp", "Duration", "Source"],
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

    latency_daily = db.get_wan_latency_daily(conn, start_ts, now)
    if latency_daily:
        print()
        lat_table = [
            [
                row["day"],
                row["wan_name"],
                row["samples"],
                f"{row['min_latency']:.1f} ms",
                f"{row['avg_latency']:.1f} ms",
                f"{row['max_latency']:.1f} ms",
            ]
            for row in latency_daily
        ]
        print(tabulate(
            lat_table,
            headers=["Date", "WAN", "Samples", "Min", "Avg", "Max"],
            tablefmt="simple",
        ))
        print("  (router-measured, not client ping)")


def _resolve_report_period(conn, period: str) -> tuple[int, str]:
    """Returns (start_ts, start_day) for the report's time window."""
    now = int(time.time())
    if period == "all":
        earliest = db.get_earliest_rollup_day(conn)
        if earliest is None:
            start_day = datetime.now(timezone.utc).date().isoformat()
            return now, start_day
        start_ts = int(
            datetime.strptime(earliest, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        )
        return start_ts, earliest
    if period.endswith("d") and period[:-1].isdigit():
        days = int(period[:-1])
        start_ts = now - days * 86400
        start_day = datetime.fromtimestamp(start_ts, tz=timezone.utc).date().isoformat()
        return start_ts, start_day
    print(f"Error: invalid --period '{period}' (use 'all' or e.g. '90d')", file=sys.stderr)
    sys.exit(1)


def cmd_report(conn, period: str, output: str | None, cfg: dict) -> None:
    start_ts, start_day = _resolve_report_period(conn, period)
    now = int(time.time())
    end_day = datetime.now(timezone.utc).date().isoformat()

    throughput_rows = [
        r for r in db.get_throughput_rollup_range(conn, start_day, end_day)
        if r["label"].startswith("WAN")
    ]
    latency_rows = db.get_latency_rollup_range(conn, start_day, end_day)
    health_events = [e for e in db.get_health_events(conn) if e["timestamp"] >= start_ts]

    if not throughput_rows:
        print("No rollup data available yet for this period. Run rollup.py first, or wait for the next daily rollup.")
        return

    period_label = "all time" if period == "all" else f"last {period}"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    summary_rows, storms = report.build_summary_rows(
        throughput_rows, latency_rows, health_events, start_ts, now,
    )

    print(f"WAN report — {period_label} ({start_day} to {end_day})\n")
    print(tabulate(
        summary_rows,
        headers=["WAN", "Availability", "Outages", "Total Downtime", "Longest Outage", "Avg Latency"],
        tablefmt="simple",
    ))

    if storms:
        print()
        print(f"Storm days (>{report.STORM_THRESHOLD} outages in a UTC day):")
        print(tabulate(
            [[s["day"], s["wan_name"], s["count"]] for s in storms],
            headers=["Day", "WAN", "Outages"],
            tablefmt="simple",
        ))

    html = report.build_html(
        period_label=period_label,
        start_day=start_day,
        end_day=end_day,
        throughput_rows=throughput_rows,
        latency_rows=latency_rows,
        health_events=health_events,
        start_ts=start_ts,
        end_ts=now,
        generated_at=generated_at,
    )

    if output:
        out_path = Path(output)
    else:
        report_dir = Path(cfg["report_dir"])
        report_dir.mkdir(parents=True, exist_ok=True)
        out_path = report_dir / f"report_{end_day}.html"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"\nReport written to {out_path}")


def cmd_ping(conn, period: str) -> None:
    seconds = PERIODS[period]
    now = int(time.time())
    start_ts = now - seconds

    rows_raw = db.get_wan_latency_in_period(conn, start_ts, now)
    if not rows_raw:
        print(f"No latency data in the last {period}.")
        return

    print(f"WAN latency history — last {period}  (router health check)\n")
    rows = [
        [
            fmt_ts(r["timestamp"]),
            r["wan_name"],
            f"{r['latency_min']:.1f} ms",
            f"{r['latency_avg']:.1f} ms",
            f"{r['latency_max']:.1f} ms",
        ]
        for r in reversed(rows_raw)
    ]
    print(tabulate(rows, headers=["Timestamp", "WAN", "Min", "Avg", "Max"], tablefmt="simple"))


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
    subs.add_parser("status", help="Alias for current")

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

    report_p = subs.add_parser("report", help="Generate an HTML trend report plus terminal summary")
    report_p.add_argument(
        "--period",
        default="all",
        metavar="all|Nd",
        help="Time period to cover: 'all' or e.g. '90d' (default: all)",
    )
    report_p.add_argument(
        "--output",
        metavar="PATH",
        help="Output HTML file path (default: <report_dir>/report_<date>.html)",
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


def fetch_remote_db(cfg: dict) -> str:
    """Return a local path to a fresh copy of the monitoring DB.

    Unlike every other command, `report` doesn't SSH over and run itself
    remotely — it always wants to generate its HTML file locally so it's
    somewhere you'll actually look at it. Since the collector normally runs
    on a different machine (the Mini) than where you'd want to read a
    report (this one), `report` instead pulls a fresh, WAL-safe snapshot of
    the remote DB back to a local temp file and reads from that. If no
    remote_host is configured, or this already *is* the remote host, it
    just uses the local DB directly.
    """
    host = cfg.get("remote_host")
    if not host or _is_local(host):
        return cfg["db_path"]

    user = cfg.get("remote_user", "rob")
    remote = f"{user}@{host}"
    remote_snapshot = "/tmp/peplink_monitor_report_snapshot.db"
    local_snapshot = str(PROJECT_DIR / "data" / ".report_snapshot.db")

    print(f"Fetching a fresh data snapshot from {remote} ...")
    snapshot_cmd = f"sqlite3 {shlex.quote(cfg['db_path'])} \".backup '{remote_snapshot}'\""
    if subprocess.run(["ssh", "-A", remote, snapshot_cmd]).returncode != 0:
        print("Error: failed to snapshot the remote database.", file=sys.stderr)
        sys.exit(1)
    if subprocess.run(["scp", "-q", f"{remote}:{remote_snapshot}", local_snapshot]).returncode != 0:
        print("Error: failed to copy the remote database snapshot.", file=sys.stderr)
        sys.exit(1)
    subprocess.run(["ssh", remote, "rm", "-f", remote_snapshot])
    return local_snapshot


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

    if args.command == "report":
        # report always generates its file locally, and always wants
        # current data — pull a fresh snapshot from the remote host
        # (regardless of --remote) rather than reading a possibly-stale
        # local DB copy.
        db_path = fetch_remote_db(cfg)
        conn = db.get_connection(db_path)
        db.init_db(conn)
        try:
            cmd_report(conn, args.period, args.output, cfg)
        finally:
            conn.close()
            if db_path != cfg["db_path"]:
                Path(db_path).unlink(missing_ok=True)
        return

    if args.remote:
        run_remote(cfg)

    conn = db.get_connection(cfg["db_path"])
    db.init_db(conn)

    try:
        if args.command in ("current", "status"):
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
