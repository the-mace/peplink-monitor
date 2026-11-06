#!/usr/bin/env python3
"""Weekly plain-text WAN summary, emailed to ``weekly_summary_email``.

The body is line-oriented ``key=value`` text so a bot can parse it while a
person can still skim it. Format is versioned (``format=peplink-weekly/1``);
bump the version when a key is renamed or removed.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import alerts
import db
from config import load_config

log = logging.getLogger(__name__)

FORMAT_VERSION = "peplink-weekly/1"
GAP_THRESHOLD_SECONDS = 11 * 60
BASELINE_WEEKS = 4


def iso(ts: int, tz: ZoneInfo) -> str:
    return datetime.fromtimestamp(ts, tz=tz).isoformat(timespec="seconds")


def fmt_dur(seconds: float) -> str:
    s = int(round(seconds))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


def quote(text: str) -> str:
    return '"' + (text or "").replace('"', "'").replace("\n", " ").strip() + '"'


def default_window(now: int, tz: ZoneInfo, days: int, through_now: bool) -> tuple[int, int]:
    """Whole local days ending at the most recent local midnight, unless through_now."""
    if through_now:
        return now - days * 86400, now
    end_dt = datetime.fromtimestamp(now, tz=tz).replace(hour=0, minute=0, second=0, microsecond=0)
    # Subtract calendar days on naive local time so a DST change inside the
    # window still lands on midnight.
    start_dt = (end_dt.replace(tzinfo=None) - timedelta(days=days)).replace(tzinfo=tz)
    return int(start_dt.timestamp()), int(end_dt.timestamp())


def drop_intervals(health_events: list[dict], start_ts: int, end_ts: int) -> list[dict]:
    """Per-WAN non-green intervals overlapping [start_ts, end_ts], clamped to it.

    Same state rules as ``report.compute_availability`` (pre-window events seed
    state; duplicate poll/log recoveries are ignored), so summed durations match
    its downtime. ``precise`` is False when the down edge came from a 5-minute
    poll rather than the router's event log.
    """
    by_wan: dict[str, list[dict]] = defaultdict(list)
    for e in health_events:
        by_wan[e["wan_name"]].append(e)

    out: list[dict] = []
    for wan, events in by_wan.items():
        events.sort(key=lambda x: (x["timestamp"], x.get("id", 0)))
        down: dict | None = None
        state_known = False

        def close(end: int, ongoing: bool) -> None:
            nonlocal down
            carried = down["start"] < start_ts
            s, e = max(down["start"], start_ts), min(end, end_ts)
            if e > s or not carried:
                out.append({**down, "start": s, "end": e, "duration": e - s,
                            "ongoing": ongoing, "carried": carried})
            down = None

        for e in events:
            ts = e["timestamp"]
            if ts > end_ts:
                break
            went_down = e["old_status"] == "green" and e["new_status"] != "green"
            came_up = e["old_status"] != "green" and e["new_status"] == "green"
            if went_down:
                if down is None:
                    down = {"wan": wan, "start": ts, "reason": (e.get("message") or "").strip(),
                            "precise": e.get("source") == "log"}
                state_known = True
            elif came_up:
                if down is not None:
                    close(ts, ongoing=False)
                elif not state_known and ts >= start_ts:
                    down = {"wan": wan, "start": start_ts - 1, "reason": "", "precise": False}
                    close(ts, ongoing=False)
                state_known = True
        if down is not None:
            close(end_ts, ongoing=True)
    out.sort(key=lambda d: (d["start"], d["wan"]))
    return out


def collection_gaps(sample_ts: list[int], start_ts: int, end_ts: int,
                    threshold: int = GAP_THRESHOLD_SECONDS) -> list[tuple[int, int]]:
    """Gaps longer than ``threshold`` between polls, including the window edges."""
    points = [start_ts] + sorted(set(sample_ts)) + [end_ts]
    return [(a, b) for a, b in zip(points, points[1:]) if b - a > threshold]


def wan_interfaces(conn) -> dict[int, str]:
    return {i["id"]: i["name"] for i in db.get_interfaces(conn)
            if str(i.get("label") or "").startswith("WAN")}


def week_stats(conn, events: list[dict], wans: list[str], ifaces: dict[int, str],
               start_ts: int, end_ts: int) -> dict:
    span = max(end_ts - start_ts, 1)
    drops = drop_intervals(events, start_ts, end_ts)
    stats = {w: {"drops": 0, "down_s": 0, "lat": [], "lat_max": None,
                 "rx": 0, "tx": 0, "peak_rx": None, "peak_tx": None}
             for w in wans}
    for d in drops:
        if d["wan"] in stats:
            stats[d["wan"]]["drops"] += 0 if d["carried"] else 1
            stats[d["wan"]]["down_s"] += d["duration"]
    for r in db.get_wan_latency_in_period(conn, start_ts, end_ts):
        s = stats.get(r["wan_name"])
        if s is None:
            continue
        s["lat"].append(r["latency_avg"])
        if s["lat_max"] is None or r["latency_max"] > s["lat_max"][0]:
            s["lat_max"] = (r["latency_max"], r["timestamp"])
    for r in db.get_throughput_in_period(conn, start_ts + 1, end_ts):
        name = ifaces.get(r["interface_id"])
        s = stats.get(name)
        if s is None:
            continue
        s["rx"] += r["delta_bytes_in"]
        s["tx"] += r["delta_bytes_out"]
        if s["peak_rx"] is None or r["mbps_in"] > s["peak_rx"][0]:
            s["peak_rx"] = (r["mbps_in"], r["timestamp"])
        if s["peak_tx"] is None or r["mbps_out"] > s["peak_tx"][0]:
            s["peak_tx"] = (r["mbps_out"], r["timestamp"])
    for s in stats.values():
        s["uptime_pct"] = 100.0 * (1 - min(s["down_s"], span) / span)
        s["lat_avg"] = sum(s["lat"]) / len(s["lat"]) if s["lat"] else None
    return {"drops": drops, "wans": stats}


def _num(v: float | None, fmt: str) -> str:
    return "na" if v is None else format(v, fmt)


def _change(cur: float, base: float, *, abs_floor: float) -> str:
    if base == 0 and cur == 0:
        return "same"
    if base == 0:
        return "new"
    if abs(cur - base) < abs_floor:
        return "same"
    ratio = cur / base
    if ratio >= 1.5:
        return "up"
    if ratio <= 0.67:
        return "down"
    return "same"


def build_summary(conn, cfg: dict, start_ts: int, end_ts: int, now: int) -> tuple[str, str]:
    """Return (subject, body) for the window [start_ts, end_ts]."""
    tz = ZoneInfo(cfg.get("alert_timezone") or cfg.get("router_timezone") or "America/New_York")
    poll = int(cfg.get("poll_interval_seconds") or 300)
    ifaces = wan_interfaces(conn)
    wans = sorted(alerts.monitored_wan_names(conn, cfg))
    events = db.get_health_events(conn, end_ts=end_ts)

    cur = week_stats(conn, events, wans, ifaces, start_ts, end_ts)
    span = end_ts - start_ts
    base_weeks = []
    for k in range(1, BASELINE_WEEKS + 1):
        b_start, b_end = start_ts - k * span, end_ts - k * span
        base_weeks.append((b_start, week_stats(conn, events, wans, ifaces, b_start, b_end)))

    full = [o for o in alerts.find_completed_total_outages(events, wans)
            if o["ended_at"] >= start_ts and o["started_at"] <= end_ts]
    full_s = sum(min(o["ended_at"], end_ts) - max(o["started_at"], start_ts) for o in full)

    sample_ts = [r[0] for r in conn.execute(
        "SELECT DISTINCT timestamp FROM readings WHERE timestamp >= ? AND timestamp <= ?",
        (start_ts, end_ts))]
    last_any = conn.execute("SELECT MAX(timestamp) FROM readings").fetchone()[0]
    gaps = collection_gaps(sample_ts, start_ts, min(end_ts, now))
    stale_age = (now - last_any) if last_any else None
    stale = stale_age is None or stale_age > GAP_THRESHOLD_SECONDS

    lines: list[str] = []
    w = cur["wans"]
    drops = cur["drops"]
    n_drops = sum(1 for d in drops if not d["carried"])

    headline = [f"{len(full)} full outage(s)"]
    for name in wans:
        headline.append(f"{name} {w[name]['uptime_pct']:.3f}% up, "
                        f"{w[name]['drops']} drop(s) ({fmt_dur(w[name]['down_s'])})")
    if gaps or stale:
        headline.append("COLLECTION ISSUE")
    lines.append("Summary: " + "; ".join(headline) + ".")
    lines.append("")
    lines.append(f"format={FORMAT_VERSION}")
    lines.append(f"window_start={iso(start_ts, tz)} window_end={iso(end_ts, tz)} "
                 f"tz={tz.key} generated={iso(now, tz)}")
    lines.append("")

    lines.append(f"[full_outages] count={len(full)} total={fmt_dur(full_s)} total_s={full_s}")
    for o in full:
        lines.append(f"full_outage start={iso(o['started_at'], tz)} end={iso(o['ended_at'], tz)} "
                     f"dur={fmt_dur(o['duration_seconds'])} dur_s={o['duration_seconds']} "
                     f"wans={','.join(o['wans'])}")
    lines.append("")

    lines.append(f"[drops] count={n_drops} listed={len(drops)}")
    for d in drops:
        lines.append(f"drop wan={d['wan']} start={iso(d['start'], tz)} end={iso(d['end'], tz)} "
                     f"dur={fmt_dur(d['duration'])} dur_s={d['duration']} "
                     f"precise={'yes' if d['precise'] else 'no'} "
                     f"ongoing={'yes' if d['ongoing'] else 'no'} "
                     f"carried={'yes' if d['carried'] else 'no'} reason={quote(d['reason'])}")
    lines.append("")

    lines.append("[links]")
    for name in wans:
        s = w[name]
        lat_max_at = iso(s["lat_max"][1], tz) if s["lat_max"] else "na"
        prx_at = iso(s["peak_rx"][1], tz) if s["peak_rx"] else "na"
        ptx_at = iso(s["peak_tx"][1], tz) if s["peak_tx"] else "na"
        lines.append(
            f"link wan={name} uptime_pct={s['uptime_pct']:.3f} down={fmt_dur(s['down_s'])} "
            f"down_s={s['down_s']} drops={s['drops']}")
        lines.append(
            f"link wan={name} lat_avg_ms={_num(s['lat_avg'], '.1f')} "
            f"lat_max_ms={_num(s['lat_max'][0] if s['lat_max'] else None, '.1f')} "
            f"lat_max_at={lat_max_at} lat_samples={len(s['lat'])}")
        lines.append(
            f"link wan={name} rx_gb={s['rx'] / 1e9:.2f} tx_gb={s['tx'] / 1e9:.2f} "
            f"peak_rx_mbps={_num(s['peak_rx'][0] if s['peak_rx'] else None, '.1f')} "
            f"peak_rx_at={prx_at} "
            f"peak_tx_mbps={_num(s['peak_tx'][0] if s['peak_tx'] else None, '.1f')} "
            f"peak_tx_at={ptx_at}")
    lines.append("")

    lines.append(f"[trends] baseline=prior_{BASELINE_WEEKS}_windows_avg "
                 f"baseline_start={iso(base_weeks[-1][0], tz)}")
    for name in wans:
        s = w[name]
        bw = [b["wans"][name] for _, b in base_weeks]
        b_drops = sum(x["drops"] for x in bw) / len(bw)
        b_down = sum(x["down_s"] for x in bw) / len(bw)
        b_lats = [x["lat_avg"] for x in bw if x["lat_avg"] is not None]
        b_lat = sum(b_lats) / len(b_lats) if b_lats else None
        b_rx = sum(x["rx"] for x in bw) / len(bw)
        lines.append(
            f"trend wan={name} drops={s['drops']} base_drops={b_drops:.1f} "
            f"change={_change(s['drops'], b_drops, abs_floor=2)} "
            f"down_s={s['down_s']} base_down_s={b_down:.0f} "
            f"change={_change(s['down_s'], b_down, abs_floor=120)}")
        lines.append(
            f"trend wan={name} lat_avg_ms={_num(s['lat_avg'], '.1f')} "
            f"base_lat_avg_ms={_num(b_lat, '.1f')} "
            f"change={_change(s['lat_avg'] or 0, b_lat or 0, abs_floor=5)} "
            f"rx_gb={s['rx'] / 1e9:.2f} base_rx_gb={b_rx / 1e9:.2f} "
            f"change={_change(s['rx'], b_rx, abs_floor=5e9)}")

    # Recurring causes: (wan, reason) drop counts vs baseline, with ET hours.
    def keyed(ds: list[dict]) -> dict[tuple[str, str], list[dict]]:
        k: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for d in ds:
            if not d["carried"]:
                k[(d["wan"], d["reason"] or "unknown")].append(d)
        return k

    cur_k = keyed(drops)
    base_k: dict[tuple[str, str], int] = defaultdict(int)
    for _, b in base_weeks:
        for key, ds in keyed(b["drops"]).items():
            base_k[key] += len(ds)
    for key in sorted(set(cur_k) | set(base_k)):
        n = len(cur_k.get(key, []))
        base = base_k.get(key, 0) / BASELINE_WEEKS
        hours = sorted({datetime.fromtimestamp(d["start"], tz=tz).hour for d in cur_k.get(key, [])})
        lines.append(
            f"pattern wan={key[0]} reason={quote(key[1])} count={n} base_count={base:.1f} "
            f"change={_change(n, base, abs_floor=2)} "
            f"hours_local={','.join(f'{h:02d}' for h in hours) or 'none'}")
    lines.append("")

    expected = span // poll
    lines.append(
        f"[collection] samples={len(sample_ts)} expected={expected} "
        f"gaps_over_11m={len(gaps)} last_sample={iso(last_any, tz) if last_any else 'na'} "
        f"last_sample_age_s={stale_age if stale_age is not None else 'na'} "
        f"stale={'yes' if stale else 'no'}")
    for a, b in gaps:
        lines.append(f"gap start={iso(a, tz)} end={iso(b, tz)} dur={fmt_dur(b - a)} dur_s={b - a}")
    lines.append("")
    lines.append("Notes: times are local (tz above). precise=no means the down edge came from a "
                 "5-minute poll, not the router log. carried=yes means the link was already "
                 "down at window_start and is not counted in drops. Packet loss is not measured. "
                 "change=up/down means at least 1.5x / at most 0.67x the baseline.")
    lines.append("-- peplink-monitor")

    start_day = datetime.fromtimestamp(start_ts, tz=tz).date().isoformat()
    end_day = datetime.fromtimestamp(end_ts - 1, tz=tz).date().isoformat()
    subject = (f"Peplink weekly {start_day}..{end_day}: {len(full)} full outage(s), "
               f"{n_drops} drop(s)" + (", COLLECTION ISSUE" if gaps or stale else ""))
    return subject, "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--send", action="store_true",
                   help="Email to weekly_summary_email (default: print only)")
    p.add_argument("--to", metavar="ADDR", help="Override the recipient (implies --send)")
    p.add_argument("--days", type=int, default=7, help="Window length in days (default: 7)")
    p.add_argument("--through-now", action="store_true",
                   help="End the window now instead of at the last local midnight")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    to_addr = (args.to or cfg.get("weekly_summary_email") or "").strip()
    send = args.send or bool(args.to)
    if send and not to_addr:
        log.info("weekly_summary_email not set; nothing to send")
        return 0

    tz = ZoneInfo(cfg.get("alert_timezone") or cfg.get("router_timezone") or "America/New_York")
    now = int(time.time())
    start_ts, end_ts = default_window(now, tz, args.days, args.through_now)

    conn = db.get_connection(cfg["db_path"])
    try:
        subject, body = build_summary(conn, cfg, start_ts, end_ts, now)
    finally:
        conn.close()

    out_dir = Path(cfg["report_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    end_day = datetime.fromtimestamp(end_ts, tz=tz).date().isoformat()
    (out_dir / f"weekly_{end_day}.txt").write_text(f"Subject: {subject}\n\n{body}", encoding="utf-8")

    if not send:
        print(f"Subject: {subject}\n\n{body}", end="")
        return 0
    alerts.send_mail(to_addr, subject, body)
    log.info("Weekly summary sent: %s", subject)
    return 0


if __name__ == "__main__":
    sys.exit(main())
