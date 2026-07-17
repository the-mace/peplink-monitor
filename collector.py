#!/usr/bin/env python3
"""SNMP + Peplink API poller for Peplink B-One. Designed to be run via cron.

SNMP (throughput) and REST API (health / latency / event log) are independent:
a failure in one path does not skip the other.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    get_cmd,
    walk_cmd,
)

import db
import peplink_api
from config import load_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# Standard MIB OID bases
OID_IF_DESCR = "1.3.6.1.2.1.2.2.1.2"
OID_IF_STATUS = "1.3.6.1.2.1.2.2.1.8"
OID_IF_HC_IN = "1.3.6.1.2.1.31.1.1.1.6"
OID_IF_HC_OUT = "1.3.6.1.2.1.31.1.1.1.10"

# Peplink enterprise MIB: WAN connection name table (index 0 = WAN 1, etc.)
OID_PEPLINK_WAN_NAMES = "1.3.6.1.4.1.23695.2.1.2.1.2"

MAX_COUNTER64 = 2 ** 64


def delta_with_rollover(current: int, previous: int) -> int:
    """Compute byte counter delta, handling 64-bit counter rollover."""
    if current >= previous:
        return current - previous
    return MAX_COUNTER64 - previous + current


def calc_mbps(delta_bytes: int, delta_seconds: float) -> float:
    if delta_seconds <= 0:
        return 0.0
    return (delta_bytes * 8) / (delta_seconds * 1_000_000)


async def _walk_oid(engine, community, transport, oid_base: str) -> dict[int, str]:
    """Walk an OID subtree, returning {if_index: value}."""
    subtree_prefix = oid_base + "."
    results = {}
    left_subtree = False
    async for err_ind, err_stat, _err_idx, var_binds in walk_cmd(
        engine,
        community,
        transport,
        ContextData(),
        ObjectType(ObjectIdentity(oid_base)),
        lexicographic_mode=False,
    ):
        if left_subtree:
            break
        if err_ind:
            raise RuntimeError(f"SNMP walk error: {err_ind}")
        if err_stat:
            raise RuntimeError(f"SNMP walk error: {err_stat.prettyPrint()}")
        for vb in var_binds:
            oid_str = str(vb[0])
            if not oid_str.startswith(subtree_prefix):
                left_subtree = True
                break
            if_index = int(oid_str.rsplit(".", 1)[-1])
            results[if_index] = vb[1].prettyPrint().strip()
    return results


async def discover_interfaces(cfg: dict) -> list[dict]:
    """Walk ifDescr and Peplink WAN table to find all interfaces and build their OID mappings."""
    engine = SnmpEngine()
    community = CommunityData(cfg["community"], mpModel=1)
    transport = await UdpTransportTarget.create(
        (cfg["host"], cfg["port"]), timeout=10, retries=3
    )

    descr_by_index = await _walk_oid(engine, community, transport, OID_IF_DESCR)
    wan_names_by_index = await _walk_oid(engine, community, transport, OID_PEPLINK_WAN_NAMES)

    # Build {name -> "WAN N"} from Peplink enterprise MIB (0-based index → WAN 1, WAN 2, …)
    wan_label_by_name = {
        name: f"WAN {idx + 1}"
        for idx, name in wan_names_by_index.items()
        if name
    }

    # Non-WAN interfaces are LAN ports — label by ascending if_index order
    lan_if_indexes = sorted(
        if_index for if_index, name in descr_by_index.items()
        if name and name not in wan_label_by_name
    )
    lan_label_by_index = {
        if_index: f"LAN {n}"
        for n, if_index in enumerate(lan_if_indexes, start=1)
    }

    interfaces = []
    for if_index, name in descr_by_index.items():
        if not name:
            continue
        label = wan_label_by_name.get(name) or lan_label_by_index.get(if_index, "")
        interfaces.append({
            "name": name,
            "if_index": if_index,
            "oid_hc_in": f"{OID_IF_HC_IN}.{if_index}",
            "oid_hc_out": f"{OID_IF_HC_OUT}.{if_index}",
            "oid_status": f"{OID_IF_STATUS}.{if_index}",
            "label": label,
        })

    log.info(
        "Discovered %d interfaces: %s",
        len(interfaces),
        [f"{i['name']} ({i['label']})" for i in interfaces],
    )
    return interfaces


async def poll_interfaces(cfg: dict, interfaces: list[dict]) -> dict[int, dict]:
    """Fetch HC in/out counters and oper status for all interfaces in one GET."""
    engine = SnmpEngine()
    community = CommunityData(cfg["community"], mpModel=1)
    transport = await UdpTransportTarget.create(
        (cfg["host"], cfg["port"]), timeout=10, retries=3
    )

    oids = []
    for iface in interfaces:
        oids.append(ObjectType(ObjectIdentity(iface["oid_hc_in"])))
        oids.append(ObjectType(ObjectIdentity(iface["oid_hc_out"])))
        oids.append(ObjectType(ObjectIdentity(iface["oid_status"])))

    err_ind, err_stat, _err_idx, var_binds = await get_cmd(
        engine, community, transport, ContextData(), *oids
    )
    if err_ind:
        raise RuntimeError(f"SNMP get error: {err_ind}")
    if err_stat:
        raise RuntimeError(f"SNMP get error: {err_stat.prettyPrint()}")

    raw: dict[str, int] = {}
    for vb in var_binds:
        try:
            raw[str(vb[0])] = int(vb[1])
        except (TypeError, ValueError):
            raw[str(vb[0])] = 0

    return {
        iface["id"]: {
            "bytes_in": raw.get(iface["oid_hc_in"], 0),
            "bytes_out": raw.get(iface["oid_hc_out"], 0),
            "oper_status": raw.get(iface["oid_status"], 2),
        }
        for iface in interfaces
    }


def poll_api(cfg: dict, conn, now: int) -> None:
    """Poll Peplink REST API for health state, latency, and event log."""
    api = peplink_api.from_config(cfg)
    if api is None:
        log.warning(
            "Peplink API credentials not configured — skipping health check and latency polling. "
            "Add peplink_api_client_id and peplink_api_client_secret to config.yaml."
        )
        return

    try:
        wan_statuses = api.get_wan_status()
        known_states = db.get_wan_health_states(conn)
        for wan in wan_statuses:
            wan_id = wan["wan_id"]
            prev = known_states.get(wan_id)
            if prev is not None and prev["status_led"] != wan["status_led"]:
                stored = db.try_save_health_event(
                    conn,
                    now,
                    wan_id,
                    wan["name"],
                    prev["status_led"],
                    wan["status_led"],
                    wan["message"],
                    source="poll",
                    commit=False,
                )
                if stored:
                    log.info(
                        "WAN health change: %s  %s → %s  (%s)",
                        wan["name"],
                        prev["status_led"],
                        wan["status_led"],
                        wan["message"],
                    )
            db.upsert_wan_health_state(
                conn,
                wan_id,
                wan["name"],
                wan["status_led"],
                wan["message"],
                wan["uptime_seconds"],
                now,
                commit=False,
            )

        poll_interval = int(cfg.get("poll_interval_seconds", 300))
        wan_latencies = api.get_wan_latency(poll_interval)
        for wan_lat in wan_latencies:
            db.save_wan_latency(
                conn,
                now,
                wan_lat["name"],
                wan_lat["latency_min_ms"],
                wan_lat["latency_avg_ms"],
                wan_lat["latency_max_ms"],
                commit=False,
            )
            log.info(
                "WAN latency: %s  min=%.1f ms  avg=%.1f ms  max=%.1f ms  (%d samples)",
                wan_lat["name"],
                wan_lat["latency_min_ms"],
                wan_lat["latency_avg_ms"],
                wan_lat["latency_max_ms"],
                wan_lat["sample_count"],
            )
    except peplink_api.PeplinkAPIError as exc:
        log.error("Peplink API poll failed: %s", exc)

    # Event log: catch sub-poll-interval WAN events that occurred between polls.
    try:
        log_events = api.fetch_event_log()
        new_count = 0
        for e in log_events:
            stored = db.try_save_log_health_event(
                conn,
                e["timestamp"],
                e["wan_name"],
                e["event_type"],
                e["detail"],
                commit=False,
            )
            if stored:
                new_count += 1
                log.info(
                    "WAN event (log): %s  %s  (%s)",
                    e["wan_name"],
                    e["event_type"],
                    e["detail"],
                )
        log.info("Event log: %d new WAN event(s) stored", new_count)
    except peplink_api.PeplinkAPIError as exc:
        log.warning("Event log fetch failed (non-fatal): %s", exc)


async def poll_snmp(cfg: dict, conn, interfaces: list[dict], now: int) -> bool:
    """Poll SNMP counters. Returns True on success, False on failure."""
    prev_readings = {
        iface["id"]: db.get_latest_reading(conn, iface["id"])
        for iface in interfaces
    }

    try:
        poll_data = await poll_interfaces(cfg, interfaces)
    except RuntimeError as exc:
        log.error("SNMP poll failed: %s", exc)
        return False

    for iface in interfaces:
        iface_id = iface["id"]
        current = poll_data[iface_id]

        db.save_reading(
            conn,
            iface_id,
            now,
            current["bytes_in"],
            current["bytes_out"],
            current["oper_status"],
            commit=False,
        )

        prev = prev_readings[iface_id]
        if prev is None:
            log.info("%s: first reading recorded", iface["name"])
            continue

        delta_s = float(now - prev["timestamp"])
        if delta_s <= 0:
            log.warning("%s: non-positive time delta, skipping throughput", iface["name"])
            continue

        delta_in = delta_with_rollover(current["bytes_in"], prev["bytes_in"])
        delta_out = delta_with_rollover(current["bytes_out"], prev["bytes_out"])
        mbps_in = calc_mbps(delta_in, delta_s)
        mbps_out = calc_mbps(delta_out, delta_s)

        db.save_throughput(
            conn,
            iface_id,
            now,
            mbps_in,
            mbps_out,
            delta_in,
            delta_out,
            delta_s,
            commit=False,
        )

        status_str = "up" if current["oper_status"] == 1 else "down"
        log.info(
            "%s: in=%.2f Mbps  out=%.2f Mbps  status=%s",
            iface["name"],
            mbps_in,
            mbps_out,
            status_str,
        )
    return True


async def main(rediscover: bool = False) -> int:
    cfg = load_config()
    conn = db.get_connection(cfg["db_path"])
    db.init_db(conn)

    log.info("Poll starting.")
    now = int(time.time())
    snmp_ok = False
    had_interfaces = False

    try:
        interfaces = db.get_interfaces(conn)
        if not interfaces or rediscover:
            reason = "forced rediscovery" if rediscover else "no cached interfaces"
            log.info("Running interface discovery (%s)...", reason)
            try:
                discovered = await discover_interfaces(cfg)
                db.save_interfaces(conn, discovered, commit=False)
                interfaces = db.get_interfaces(conn)
            except RuntimeError as exc:
                log.error("Interface discovery failed: %s", exc)
                interfaces = db.get_interfaces(conn)

        had_interfaces = bool(interfaces)
        if interfaces:
            snmp_ok = await poll_snmp(cfg, conn, interfaces, now)
        else:
            log.warning("No interfaces available — skipping SNMP poll")

        # Always attempt API even when SNMP fails (and vice versa).
        poll_api(cfg, conn, now)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    if not snmp_ok and had_interfaces:
        log.warning("Poll complete with SNMP errors (API path still attempted).")
        log.info("Poll complete.")
        return 1

    log.info("Poll complete.")
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Poll Peplink router via SNMP + REST API")
    p.add_argument(
        "--rediscover",
        action="store_true",
        help="Re-run SNMP interface discovery and refresh cached OIDs/labels",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    raise SystemExit(asyncio.run(main(rediscover=args.rediscover)))
