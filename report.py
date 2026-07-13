"""Builds the HTML trend report and terminal summary for `cli.py report`.

Reads from the throughput_daily / latency_daily rollup tables (fast
regardless of how much raw history has accumulated) plus health_events
directly (small enough to scan in full forever). No templating engine —
plain string building, matching the project's zero-extra-dependency style.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone


STORM_THRESHOLD = 8

# Color identity is per-WAN-name, not per-rank, so a WAN keeps the same
# color across every report generated over time. Validated pairs (light,
# dark) from the project's dataviz palette; fallback list covers any WAN
# not explicitly named here, in the palette's fixed hue order.
WAN_COLORS = {
    "Spectrum": ("#2a78d6", "#3987e5"),
    "Starlink": ("#eb6834", "#d95926"),
}
_FALLBACK_COLORS = [
    ("#1baf7a", "#199e70"),
    ("#e34948", "#e66767"),
    ("#4a3aa7", "#9085e9"),
    ("#e87ba4", "#d55181"),
]


def _wan_color(wan_name: str, seen: list[str]) -> tuple[str, str]:
    if wan_name in WAN_COLORS:
        return WAN_COLORS[wan_name]
    unknowns = [w for w in seen if w not in WAN_COLORS]
    idx = unknowns.index(wan_name) % len(_FALLBACK_COLORS)
    return _FALLBACK_COLORS[idx]


def _month(day: str) -> str:
    return day[:7]


def _fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def _fmt_gb(bytes_val: float) -> str:
    return f"{bytes_val / 1_073_741_824:.1f} GB"


def detect_storms(health_events: list[dict], threshold: int = STORM_THRESHOLD) -> list[dict]:
    """Flag UTC days where a WAN's went-down count exceeds `threshold`."""
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for e in health_events:
        if e["old_status"] == "green" and e["new_status"] != "green":
            day = datetime.fromtimestamp(e["timestamp"], tz=timezone.utc).date().isoformat()
            counts[(day, e["wan_name"])] += 1
    return [
        {"day": day, "wan_name": wan, "count": cnt}
        for (day, wan), cnt in sorted(counts.items())
        if cnt > threshold
    ]


def compute_availability(health_events: list[dict], start_ts: int, end_ts: int) -> dict[str, dict]:
    """Per-WAN: event_count, total_downtime_seconds, longest_outage_seconds,
    longest_outage_day, availability_pct over [start_ts, end_ts].
    """
    by_wan: dict[str, list[dict]] = defaultdict(list)
    for e in health_events:
        by_wan[e["wan_name"]].append(e)

    result: dict[str, dict] = {}
    span = max(end_ts - start_ts, 1)
    for wan_name, events in by_wan.items():
        events.sort(key=lambda x: x["timestamp"])
        down_at: int | None = None
        total_down = 0.0
        longest = 0.0
        longest_day = None
        event_count = 0
        for e in events:
            if e["old_status"] == "green" and e["new_status"] != "green":
                down_at = e["timestamp"]
                event_count += 1
            elif e["old_status"] != "green" and e["new_status"] == "green" and down_at is not None:
                dur = e["timestamp"] - down_at
                total_down += dur
                if dur > longest:
                    longest = dur
                    longest_day = datetime.fromtimestamp(down_at, tz=timezone.utc).date().isoformat()
                down_at = None
        result[wan_name] = {
            "event_count": event_count,
            "total_downtime_seconds": total_down,
            "longest_outage_seconds": longest,
            "longest_outage_day": longest_day,
            "availability_pct": 100.0 * (1 - total_down / span),
        }
    return result


def hour_histogram(health_events: list[dict], exclude_days: set[str]) -> dict[str, dict[int, int]]:
    """Per-WAN went-down counts by UTC hour, excluding storm days."""
    result: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    for e in health_events:
        if e["old_status"] != "green" or e["new_status"] == "green":
            continue
        dt = datetime.fromtimestamp(e["timestamp"], tz=timezone.utc)
        if dt.date().isoformat() in exclude_days:
            continue
        result[e["wan_name"]][dt.hour] += 1
    return {wan: dict(hours) for wan, hours in result.items()}


def _monthly_throughput(throughput_rows: list[dict]) -> dict[str, dict[str, dict]]:
    """{month: {wan_name: {total_in, total_out, peak_in, avg_in, days}}}"""
    agg: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
        "total_in": 0, "total_out": 0, "peak_in": 0.0, "peak_out": 0.0, "days": 0,
    }))
    for r in throughput_rows:
        m = agg[_month(r["day"])][r["name"]]
        m["total_in"] += r["total_in"]
        m["total_out"] += r["total_out"]
        m["peak_in"] = max(m["peak_in"], r["peak_in"])
        m["peak_out"] = max(m["peak_out"], r["peak_out"])
        m["days"] += 1
    return agg


def _monthly_latency(latency_rows: list[dict]) -> dict[str, dict[str, dict]]:
    """{month: {wan_name: {avg_latency, min_latency, max_latency}}}"""
    sums: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(lambda: {
        "avg_sum": 0.0, "n": 0, "min_latency": None, "max_latency": None,
    }))
    for r in latency_rows:
        s = sums[_month(r["day"])][r["wan_name"]]
        s["avg_sum"] += r["avg_latency"]
        s["n"] += 1
        s["min_latency"] = r["min_latency"] if s["min_latency"] is None else min(s["min_latency"], r["min_latency"])
        s["max_latency"] = r["max_latency"] if s["max_latency"] is None else max(s["max_latency"], r["max_latency"])
    out: dict[str, dict[str, dict]] = defaultdict(dict)
    for month, wans in sums.items():
        for wan, s in wans.items():
            out[month][wan] = {
                "avg_latency": s["avg_sum"] / s["n"] if s["n"] else 0.0,
                "min_latency": s["min_latency"] or 0.0,
                "max_latency": s["max_latency"] or 0.0,
            }
    return out


def _monthly_storm_counts(health_events: list[dict]) -> dict[str, dict[str, int]]:
    """{month: {wan_name: went_down_count}} — for the trend chart."""
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for e in health_events:
        if e["old_status"] == "green" and e["new_status"] != "green":
            m = _month(datetime.fromtimestamp(e["timestamp"], tz=timezone.utc).date().isoformat())
            counts[m][e["wan_name"]] += 1
    return counts


def build_summary_rows(
    throughput_rows: list[dict],
    latency_rows: list[dict],
    health_events: list[dict],
    start_ts: int,
    end_ts: int,
) -> tuple[list[list], list[dict]]:
    """Returns (tabulate-ready rows, storm list) for the terminal summary."""
    avail = compute_availability(health_events, start_ts, end_ts)
    storms = detect_storms(health_events)

    latest_latency: dict[str, dict] = {}
    for r in latency_rows:
        latest_latency[r["wan_name"]] = r  # rows are ordered by day, last wins per wan is fine for "any sample"

    monthly_lat = _monthly_latency(latency_rows)
    overall_avg_latency: dict[str, list[float]] = defaultdict(list)
    for month_data in monthly_lat.values():
        for wan, v in month_data.items():
            overall_avg_latency[wan].append(v["avg_latency"])

    wan_names = sorted({r["name"] for r in throughput_rows})
    rows = []
    for wan in wan_names:
        a = avail.get(wan, {"event_count": 0, "total_downtime_seconds": 0, "longest_outage_seconds": 0, "availability_pct": 100.0})
        lat_vals = overall_avg_latency.get(wan, [])
        avg_lat = sum(lat_vals) / len(lat_vals) if lat_vals else None
        rows.append([
            wan,
            f"{a['availability_pct']:.3f}%",
            a["event_count"],
            _fmt_duration(a["total_downtime_seconds"]),
            _fmt_duration(a["longest_outage_seconds"]),
            f"{avg_lat:.1f} ms" if avg_lat is not None else "-",
        ])
    return rows, storms


def _svg_month_bars(monthly: dict[str, dict[str, int]], wan_names: list[str], height: int = 220) -> str:
    """Grouped/stacked bar chart of a per-month, per-wan integer count."""
    months = sorted(monthly.keys())
    if not months:
        return "<p class='muted'>No data.</p>"
    max_val = max((sum(monthly[m].get(w, 0) for w in wan_names) for m in months), default=1) or 1

    W, H = 860, height
    pad_l, pad_r, pad_t, pad_b = 34, 10, 10, 30
    plot_w, plot_h = W - pad_l - pad_r, H - pad_t - pad_b
    slot_w = plot_w / len(months)
    bar_w = max(4, slot_w * 0.6)

    def y(v: float) -> float:
        return pad_t + plot_h - (v / max_val) * plot_h

    parts = [f'<svg width="100%" viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">']
    for frac in (0, 0.5, 1.0):
        v = max_val * frac
        yy = y(v)
        parts.append(f'<line x1="{pad_l}" x2="{W - pad_r}" y1="{yy:.1f}" y2="{yy:.1f}" class="gridline"/>')
        parts.append(f'<text x="{pad_l - 8}" y="{yy + 4:.1f}" text-anchor="end" class="axis-label">{v:.0f}</text>')

    for i, month in enumerate(months):
        x = pad_l + i * slot_w + (slot_w - bar_w) / 2
        parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{H - 8}" text-anchor="middle" class="axis-label">{month}</text>')
        cursor = pad_t + plot_h
        for wan in wan_names:
            val = monthly[month].get(wan, 0)
            if val <= 0:
                continue
            h = (val / max_val) * plot_h
            color_var = f"var(--wan-{wan.lower()})"
            parts.append(
                f'<rect x="{x:.1f}" y="{cursor - h:.1f}" width="{bar_w:.1f}" height="{max(0, h - 1):.1f}" '
                f'rx="2" fill="{color_var}"><title>{month} {wan}: {val}</title></rect>'
            )
            cursor -= h
    parts.append("</svg>")
    return "".join(parts)


def _svg_hour_chart(hours: dict[int, int], wan_name: str, height: int = 200) -> str:
    W, H = 860, height
    pad_l, pad_r, pad_t, pad_b = 34, 10, 10, 30
    plot_w, plot_h = W - pad_l - pad_r, H - pad_t - pad_b
    max_val = max(hours.values(), default=1) or 1
    slot_w = plot_w / 24
    bar_w = slot_w * 0.7
    color_var = f"var(--wan-{wan_name.lower()})"

    parts = [f'<svg width="100%" viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">']
    for frac in (0, 0.5, 1.0):
        v = max_val * frac
        yy = pad_t + plot_h - (v / max_val) * plot_h
        parts.append(f'<line x1="{pad_l}" x2="{W - pad_r}" y1="{yy:.1f}" y2="{yy:.1f}" class="gridline"/>')
        parts.append(f'<text x="{pad_l - 8}" y="{yy + 4:.1f}" text-anchor="end" class="axis-label">{v:.0f}</text>')

    for hr in range(24):
        val = hours.get(hr, 0)
        x = pad_l + hr * slot_w + (slot_w - bar_w) / 2
        h = (val / max_val) * plot_h if val else 0
        parts.append(
            f'<rect x="{x:.1f}" y="{pad_t + plot_h - h:.1f}" width="{bar_w:.1f}" height="{max(0, h):.1f}" '
            f'rx="2" fill="{color_var}"><title>{hr:02d}:00 UTC: {val} outages</title></rect>'
        )
        if hr % 2 == 0:
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{H - 8}" text-anchor="middle" class="axis-label">{hr:02d}</text>')
    parts.append("</svg>")
    return "".join(parts)


def _svg_latency_lines(monthly_lat: dict[str, dict[str, dict]], wan_names: list[str], height: int = 220) -> str:
    months = sorted(monthly_lat.keys())
    if not months:
        return "<p class='muted'>No data.</p>"
    all_vals = [monthly_lat[m][w]["avg_latency"] for m in months for w in wan_names if w in monthly_lat[m]]
    max_val = (max(all_vals) * 1.2) if all_vals else 10

    W, H = 860, height
    pad_l, pad_r, pad_t, pad_b = 34, 10, 10, 30
    plot_w, plot_h = W - pad_l - pad_r, H - pad_t - pad_b

    def x(i: int) -> float:
        return pad_l + (i / max(len(months) - 1, 1)) * plot_w

    def y(v: float) -> float:
        return pad_t + plot_h - (v / max_val) * plot_h

    parts = [f'<svg width="100%" viewBox="0 0 {W} {H}" preserveAspectRatio="xMidYMid meet">']
    for frac in (0, 0.5, 1.0):
        v = max_val * frac
        yy = y(v)
        parts.append(f'<line x1="{pad_l}" x2="{W - pad_r}" y1="{yy:.1f}" y2="{yy:.1f}" class="gridline"/>')
        parts.append(f'<text x="{pad_l - 8}" y="{yy + 4:.1f}" text-anchor="end" class="axis-label">{v:.0f}ms</text>')
    for i, m in enumerate(months):
        parts.append(f'<text x="{x(i):.1f}" y="{H - 8}" text-anchor="middle" class="axis-label">{m}</text>')

    for wan in wan_names:
        color_var = f"var(--wan-{wan.lower()})"
        pts = [(i, monthly_lat[m][wan]["avg_latency"]) for i, m in enumerate(months) if wan in monthly_lat[m]]
        if not pts:
            continue
        d = " ".join(f'{"M" if i == 0 else "L"}{x(idx):.1f} {y(v):.1f}' for i, (idx, v) in enumerate(pts))
        parts.append(f'<path d="{d}" fill="none" stroke="{color_var}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>')
        for idx, v in pts:
            month = months[idx]
            parts.append(f'<circle cx="{x(idx):.1f}" cy="{y(v):.1f}" r="4" fill="{color_var}"><title>{month} {wan}: {v:.1f}ms avg</title></circle>')
    parts.append("</svg>")
    return "".join(parts)


def build_html(
    *,
    period_label: str,
    start_day: str,
    end_day: str,
    throughput_rows: list[dict],
    latency_rows: list[dict],
    health_events: list[dict],
    start_ts: int,
    end_ts: int,
    generated_at: str,
) -> str:
    wan_names = sorted({r["name"] for r in throughput_rows})
    for w in wan_names:
        _wan_color(w, wan_names)  # populate fallback assignment deterministically

    avail = compute_availability(health_events, start_ts, end_ts)
    storms = detect_storms(health_events)
    storm_days = {s["day"] for s in storms}
    hours = hour_histogram(health_events, exclude_days=storm_days)
    monthly_tp = _monthly_throughput(throughput_rows)
    monthly_lat = _monthly_latency(latency_rows)
    monthly_storms = _monthly_storm_counts(health_events)

    # CSS variables for each WAN's validated color pair (light, dark)
    wan_css_light = []
    wan_css_dark = []
    for w in wan_names:
        light, dark = _wan_color(w, wan_names)
        wan_css_light.append(f"--wan-{w.lower()}: {light};")
        wan_css_dark.append(f"--wan-{w.lower()}: {dark};")

    kpi_html = []
    for wan in wan_names:
        a = avail.get(wan, {"availability_pct": 100.0, "event_count": 0, "total_downtime_seconds": 0})
        kpi_html.append(f"""
        <div class="kpi">
          <div class="label">{wan} availability</div>
          <div class="value" style="color: var(--wan-{wan.lower()})">{a['availability_pct']:.3f}%</div>
          <div class="note">{a['event_count']} outages, {_fmt_duration(a['total_downtime_seconds'])} total downtime</div>
        </div>""")

    storm_rows = "".join(
        f"<tr><td>{s['day']}</td><td>{s['wan_name']}</td><td class='num'>{s['count']}</td></tr>"
        for s in storms
    ) or "<tr><td colspan='3' class='muted'>None detected (threshold: more than " + str(STORM_THRESHOLD) + " outages in a UTC day)</td></tr>"

    monthly_traffic_rows = ""
    for month in sorted(monthly_tp.keys()):
        cells = "".join(
            f"<td class='num'>{_fmt_gb(monthly_tp[month].get(w, {}).get('total_in', 0) + monthly_tp[month].get(w, {}).get('total_out', 0))}</td>"
            for w in wan_names
        )
        monthly_traffic_rows += f"<tr><td>{month}</td>{cells}</tr>"
    monthly_traffic_header = "".join(f"<th class='num'>{w}</th>" for w in wan_names)

    hour_sections = ""
    for wan in wan_names:
        wan_hours = hours.get(wan, {})
        if sum(wan_hours.values()) < 5:
            continue
        hour_sections += f"""
        <h2>{wan} outages by hour of day (UTC)</h2>
        <div class="section-sub">Storm days (&gt;{STORM_THRESHOLD}/day) excluded so the routine pattern isn't swamped by one-off events.</div>
        <div class="card">{_svg_hour_chart(wan_hours, wan)}</div>"""

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>WAN Report {end_day}</title>
<style>
:root {{
  --surface: #ffffff; --surface-2: #f6f7f9; --border: #e3e6eb;
  --ink: #1a1d23; --ink-2: #4b5261; --ink-muted: #7a8195;
  {" ".join(wan_css_light)}
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --surface: #181a20; --surface-2: #20232b; --border: #2c313c;
    --ink: #e8eaee; --ink-2: #b7bcc7; --ink-muted: #828a9c;
    {" ".join(wan_css_dark)}
  }}
}}
* {{ box-sizing: border-box; }}
body {{
  background: var(--surface); color: var(--ink);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  margin: 0; padding: 32px 20px 64px; max-width: 920px; margin-inline: auto;
}}
h1 {{ font-size: 22px; margin: 0 0 4px; }}
.sub {{ color: var(--ink-muted); font-size: 14px; margin-bottom: 32px; }}
h2 {{ font-size: 15px; margin: 40px 0 4px; }}
.section-sub {{ color: var(--ink-muted); font-size: 13px; margin-bottom: 16px; }}
.card {{ background: var(--surface-2); border: 1px solid var(--border); border-radius: 10px; padding: 18px 20px; margin-bottom: 14px; }}
.kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; }}
.kpi {{ background: var(--surface-2); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }}
.kpi .label {{ font-size: 12px; color: var(--ink-muted); margin-bottom: 6px; }}
.kpi .value {{ font-size: 22px; font-weight: 600; }}
.kpi .note {{ font-size: 12px; color: var(--ink-2); margin-top: 4px; }}
svg {{ overflow: visible; font-family: inherit; }}
.axis-label {{ fill: var(--ink-muted); font-size: 11px; }}
.gridline {{ stroke: var(--border); stroke-width: 1; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid var(--border); }}
th {{ color: var(--ink-muted); font-weight: 500; font-size: 12px; }}
td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
.muted {{ color: var(--ink-muted); }}
</style>
</head>
<body>

<h1>WAN Reliability Report</h1>
<div class="sub">{period_label} ({start_day} to {end_day}) &middot; generated {generated_at}</div>

<div class="kpi-row">{"".join(kpi_html)}</div>

<h2>Outage events per month</h2>
<div class="section-sub">Went-down events by WAN, bucketed by month so this stays readable no matter how much history accumulates.</div>
<div class="card">{_svg_month_bars(monthly_storms, wan_names)}</div>

{hour_sections}

<h2>Monthly latency (router-measured)</h2>
<div class="card">{_svg_latency_lines(monthly_lat, wan_names)}</div>

<h2>Monthly traffic</h2>
<div class="card">
<table>
<tr><th>Month</th>{monthly_traffic_header}</tr>
{monthly_traffic_rows}
</table>
</div>

<h2>Storm days</h2>
<div class="section-sub">UTC days where a WAN logged more than {STORM_THRESHOLD} outages &mdash; usually a settling-in period or an anomaly worth a closer look, not a representative day.</div>
<div class="card">
<table>
<tr><th>Day</th><th>WAN</th><th class="num">Outages</th></tr>
{storm_rows}
</table>
</div>

</body>
</html>
"""
