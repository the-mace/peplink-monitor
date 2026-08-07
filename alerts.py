"""Total-network-outage detection and email alerts.

A total outage is any interval when every monitored WAN is non-green
(health-check failed / disconnected). The outage ends when the first WAN
recovers — that is when internet can resume via multi-WAN failover.

Alerts are sent only after recovery, using the system ``mail`` command
(macOS Mail / sendmail on both the Mini and the laptop).
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

import db

log = logging.getLogger(__name__)


def find_completed_total_outages(
    health_events: list[dict],
    wan_names: set[str] | list[str],
) -> list[dict]:
    """Return completed intervals when every WAN in ``wan_names`` was down.

    Each result dict:
      started_at, ended_at, duration_seconds, wans (list[str]),
      start_messages (dict wan -> message at down transition)

    WANs are assumed up until their first observed event. Only completed
    outages are returned (still-open total outages are omitted until recovery).
    """
    wans = sorted(set(wan_names))
    if not wans:
        return []

    # True = healthy (green).
    up = {w: True for w in wans}
    last_down_msg: dict[str, str] = {}
    total_down_at: int | None = None
    start_messages: dict[str, str] = {}
    completed: list[dict] = []

    events = sorted(
        (e for e in health_events if e.get("wan_name") in up),
        key=lambda e: (e["timestamp"], e.get("id", 0)),
    )

    for e in events:
        wan = e["wan_name"]
        was_up = up[wan]
        now_up = e.get("new_status") == "green"
        if was_up == now_up:
            continue
        up[wan] = now_up
        if not now_up:
            last_down_msg[wan] = (e.get("message") or "").strip()

        all_down = not any(up.values())
        if all_down and total_down_at is None:
            total_down_at = int(e["timestamp"])
            start_messages = {
                w: last_down_msg.get(w, "")
                for w in wans
                if not up[w]
            }
        elif not all_down and total_down_at is not None:
            ended = int(e["timestamp"])
            completed.append(
                {
                    "started_at": total_down_at,
                    "ended_at": ended,
                    "duration_seconds": max(0, ended - total_down_at),
                    "wans": list(wans),
                    "start_messages": dict(start_messages),
                }
            )
            total_down_at = None
            start_messages = {}

    return completed


def _fmt_local(ts: int, tz_name: str) -> str:
    """Local wall time without GNU strftime flags (portable on macOS)."""
    tz = ZoneInfo(tz_name)
    dt = datetime.fromtimestamp(ts, tz=tz)
    hour12 = dt.hour % 12 or 12
    # e.g. Sunday, July 19, 2026 at 7:50:37 AM EDT
    return (
        f"{dt.strftime('%A, %B')} {dt.day}, {dt.year} "
        f"at {hour12}:{dt.strftime('%M:%S')} {dt.strftime('%p %Z')}"
    )


def _fmt_duration(seconds: int) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s" if s else f"{m}m"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    parts = [f"{h}h"]
    if m:
        parts.append(f"{m}m")
    if s and not m:
        parts.append(f"{s}s")
    elif s:
        parts.append(f"{s}s")
    return " ".join(parts)


def format_outage_email(
    outage: dict,
    tz_name: str,
) -> tuple[str, str]:
    """Return (subject, body) for a recovered total outage, times in local TZ."""
    started = _fmt_local(outage["started_at"], tz_name)
    ended = _fmt_local(outage["ended_at"], tz_name)
    duration = _fmt_duration(outage["duration_seconds"])
    wans = ", ".join(outage.get("wans") or [])

    subject = f"Network outage recovered — {duration}"

    lines = [
        "A total network outage (all WANs down at once) has ended.",
        "",
        f"Started:  {started}",
        f"Ended:    {ended}",
        f"Duration: {duration}",
        f"WANs:     {wans}",
    ]
    msgs = outage.get("start_messages") or {}
    detail_lines = [f"  - {w}: {m or '(no message)'}" for w, m in sorted(msgs.items())]
    if detail_lines:
        lines.append("")
        lines.append("Messages at outage start:")
        lines.extend(detail_lines)
    lines.append("")
    lines.append(
        "Duration is the time every WAN was down — internet can resume when "
        "the first WAN recovers."
    )
    lines.append("")
    lines.append("— peplink-monitor")
    return subject, "\n".join(lines) + "\n"


def send_mail(to_addr: str, subject: str, body: str, *, mail_cmd: str = "mail") -> None:
    """Send via the system mailer (``mail`` on macOS with Mail.app configured)."""
    result = subprocess.run(
        [mail_cmd, "-s", subject, to_addr],
        input=body,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(
            f"mail command failed (exit {result.returncode}): {err or 'no output'}"
        )


def monitored_wan_names(conn, cfg: dict | None = None) -> set[str]:
    """WANs in the multi-WAN pool used for total-outage detection.

    Prefer an explicit ``alert_wan_names`` list in config. Otherwise use
    SNMP interfaces labeled ``WAN *`` (e.g. Spectrum / Starlink), so
    disabled Peplink ports (USB, Wi-Fi WAN, VLAN) are not required to be
    down for a \"total\" outage.
    """
    cfg = cfg or {}
    explicit = cfg.get("alert_wan_names") or []
    if isinstance(explicit, str):
        explicit = [explicit]
    names = {n.strip() for n in explicit if str(n).strip()}
    if names:
        return names

    ifaces = db.get_interfaces(conn)
    names = {
        i["name"]
        for i in ifaces
        if i.get("name") and str(i.get("label") or "").startswith("WAN")
    }
    if names:
        return names

    # Last resort: health-state rows that are not empty/disabled.
    skip_leds = {"empty", "gray", "disabled"}
    states = db.get_wan_health_states(conn)
    return {
        s["wan_name"]
        for s in states.values()
        if s.get("wan_name")
        and str(s.get("status_led") or "").lower() not in skip_leds
    }


def process_total_outage_alerts(cfg: dict, conn, now: int) -> list[dict]:
    """Detect newly completed total outages and email about recent ones.

    Returns the list of outages that were emailed this call.
    Historical completed outages are recorded as notified without email so
    enabling the feature does not spam inbox with months of past events.
    """
    to_addr = (cfg.get("alert_email") or "").strip()
    if not to_addr:
        log.debug("alert_email not set — skipping total-outage notifications")
        return []

    wans = monitored_wan_names(conn, cfg)
    if len(wans) < 1:
        log.warning("No monitored WANs for total-outage alerts — skipping")
        return []
    log.debug("Total-outage pool: %s", ", ".join(sorted(wans)))

    events = db.get_health_events(conn)
    completed = find_completed_total_outages(events, wans)
    if not completed:
        return []

    tz_name = cfg.get("alert_timezone") or cfg.get("router_timezone") or "America/New_York"
    min_dur = int(cfg.get("alert_min_duration_seconds") or 0)
    poll_interval = int(cfg.get("poll_interval_seconds") or 300)
    # Only email outages that ended recently; seed older ones as already notified.
    lookback = max(poll_interval * 3, 900)

    mailed: list[dict] = []
    for outage in completed:
        started = int(outage["started_at"])
        ended = int(outage["ended_at"])
        if db.is_total_outage_notified(conn, started, ended):
            continue

        duration = int(outage["duration_seconds"])
        recent = ended >= (now - lookback)
        should_mail = recent and duration >= min_dur

        if should_mail:
            subject, body = format_outage_email(outage, tz_name)
            try:
                send_mail(to_addr, subject, body)
                log.info(
                    "Emailed total-outage alert to %s: %s → %s (%s)",
                    to_addr,
                    started,
                    ended,
                    _fmt_duration(duration),
                )
                mailed.append(outage)
            except Exception as exc:
                # Leave un-notified so the next poll retries.
                log.error("Failed to send total-outage email: %s", exc)
                continue

        db.mark_total_outage_notified(
            conn,
            started,
            ended,
            duration,
            now,
            emailed=should_mail,
            commit=False,
        )
        if not should_mail:
            log.info(
                "Recorded historical total outage without email: %s → %s (%s)",
                started,
                ended,
                _fmt_duration(duration),
            )

    return mailed
