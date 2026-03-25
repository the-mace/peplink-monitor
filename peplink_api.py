"""Peplink local REST API client for WAN health status polling."""

import datetime
import json
import logging
import re
import ssl
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

# Matches WAN event lines from /api/status.log, e.g.:
#   "Mar 24 11:44:34 WAN: Starlink (Priority 1) connected (100.75.195.71)"
#   "Mar 24 11:44:14 WAN: Starlink (Priority 1) disconnected (WAN failed DNS test)"
_LOG_WAN_RE = re.compile(
    r"^(\w{3}\s+\d+\s+\d+:\d+:\d+)\s+WAN:\s+(.+?)\s+\(Priority\s+(\d+)\)\s+"
    r"(connected|disconnected)\s+\((.+)\)$"
)

_LOG_MONTHS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _parse_log_ts(ts_str: str) -> int:
    """Parse 'Mon DD HH:MM:SS' to a Unix timestamp, assuming current year.

    Handles the Dec→Jan year rollover: if the log shows December but the
    current month is January, the event is from last year.
    """
    parts = ts_str.split()
    month = _LOG_MONTHS[parts[0]]
    day = int(parts[1])
    h, m, s = (int(x) for x in parts[2].split(":"))
    now = datetime.datetime.now(datetime.timezone.utc)
    year = now.year
    if month == 12 and now.month == 1:
        year -= 1
    dt = datetime.datetime(year, month, day, h, m, s, tzinfo=datetime.timezone.utc)
    return int(dt.timestamp())


class PeplinkAPIError(Exception):
    pass


class PeplinkAPI:
    """Client for the Peplink local device REST API (firmware 8+)."""

    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        verify_ssl: bool = False,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._client_id = client_id
        self._client_secret = client_secret
        self._verify_ssl = verify_ssl
        self._access_token: str | None = None
        self._token_expiry: datetime.datetime | None = None
        self._ssl_ctx = self._make_ssl_ctx()

    def _make_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context()
        if not self._verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _do_request(self, method: str, path: str, body: dict | None = None) -> dict:
        url = f"{self._base_url}{path}"
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"} if data else {}
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, context=self._ssl_ctx, timeout=10) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode(errors="replace")
            raise PeplinkAPIError(f"HTTP {exc.code}: {body_text}") from exc
        except OSError as exc:
            raise PeplinkAPIError(f"Connection error: {exc}") from exc

        if result.get("stat") != "ok":
            raise PeplinkAPIError(
                f"API error {result.get('code')}: {result.get('message')}"
            )
        return result["response"]

    def _grant_token(self) -> None:
        log.debug("Requesting Peplink API access token")
        resp = self._do_request("POST", "/api/auth.token.grant", {
            "clientId": self._client_id,
            "clientSecret": self._client_secret,
            "scope": "api.read-only",
        })
        self._access_token = resp["accessToken"]
        expires_in = int(resp.get("expiresIn", 172800))
        # Refresh 10 minutes before actual expiry
        self._token_expiry = (
            datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(seconds=expires_in - 600)
        )
        log.info("Peplink API token granted (expires in %ds)", expires_in)

    def _ensure_token(self) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        if self._access_token is None or (
            self._token_expiry is not None and now >= self._token_expiry
        ):
            self._grant_token()
        return self._access_token  # type: ignore[return-value]

    def _get_with_retry(self, path: str) -> dict:
        """GET a path, retrying once on 401 with a fresh token."""
        token = self._ensure_token()
        try:
            return self._do_request("GET", f"{path}?accessToken={token}")
        except PeplinkAPIError as exc:
            if "401" in str(exc):
                log.warning("Peplink API token rejected — re-authenticating")
                self._access_token = None
                token = self._ensure_token()
                return self._do_request("GET", f"{path}?accessToken={token}")
            raise

    def get_wan_status(self) -> list[dict[str, Any]]:
        """Return per-WAN status dicts from GET /api/status.wan.connection.

        Each dict contains: wan_id, name, status_led, message, uptime_seconds, enabled.
        WANs are returned in priority order as given by the API's 'order' key.
        """
        resp = self._get_with_retry("/api/status.wan.connection")

        order = resp.get("order", [])
        result = []
        for wan_id in order:
            entry = resp.get(str(wan_id))
            if entry is None:
                continue
            result.append({
                "wan_id": wan_id,
                "name": entry.get("name", f"WAN {wan_id}"),
                "status_led": entry.get("statusLed", ""),
                "message": entry.get("message", ""),
                "uptime_seconds": int(entry.get("uptime", 0)),
                "enabled": bool(entry.get("enable", False)),
            })
        return result


    def get_wan_latency(self, poll_interval_seconds: int = 300) -> list[dict[str, Any]]:
        """Return per-WAN latency stats from GET /api/status.wan.latency.

        Computes min/avg/max over the most recent samples covering the last
        poll_interval_seconds window (using the response's pointInterval to
        determine sample count).  Only WANs with at least one valid sample
        are returned.

        Each dict contains: wan_id, name, latency_min_ms, latency_avg_ms,
        latency_max_ms, sample_count.
        """
        resp = self._get_with_retry("/api/status.wan.latency")

        order = resp.get("order", [])
        result = []
        for wan_id in order:
            entry = resp.get(str(wan_id))
            if entry is None:
                continue
            latency = entry.get("latency", {})
            data = latency.get("data", [])
            if not data:
                continue

            point_interval = int(latency.get("pointInterval", 10)) or 10
            num_samples = max(1, poll_interval_seconds // point_interval)
            recent = [v for v in data[-num_samples:] if v is not None and v > 0]
            if not recent:
                continue

            result.append({
                "wan_id": wan_id,
                "name": entry.get("name", f"WAN {wan_id}"),
                "latency_min_ms": float(min(recent)),
                "latency_avg_ms": float(sum(recent) / len(recent)),
                "latency_max_ms": float(max(recent)),
                "sample_count": len(recent),
            })
        return result


    def fetch_event_log(self) -> list[dict[str, Any]]:
        """Fetch /api/status.log and return parsed WAN events, newest-first.

        Only lines starting with 'WAN:' are returned; API:, Admin:, System:
        lines are ignored.

        Each dict contains: timestamp (Unix int), wan_name (str), priority
        (int), event_type ('connected' or 'disconnected'), detail (IP address
        or failure reason string).
        """
        resp = self._get_with_retry("/api/status.log")
        log_lines = resp.get("log", [])
        events = []
        for line in log_lines:
            line = line.strip()
            m = _LOG_WAN_RE.match(line)
            if not m:
                continue
            ts_str, wan_name, priority, event_type, detail = m.groups()
            try:
                ts = _parse_log_ts(ts_str)
            except (KeyError, ValueError) as exc:
                log.warning("Could not parse log timestamp %r: %s", ts_str, exc)
                continue
            events.append({
                "timestamp": ts,
                "wan_name": wan_name,
                "priority": int(priority),
                "event_type": event_type,
                "detail": detail,
            })
        return events  # Router returns newest-first


def from_config(cfg: dict) -> "PeplinkAPI | None":
    """Build a PeplinkAPI from config dict, or None if credentials are missing."""
    client_id = cfg.get("peplink_api_client_id", "").strip()
    client_secret = cfg.get("peplink_api_client_secret", "").strip()
    if not client_id or not client_secret:
        return None
    base_url = f"https://{cfg['host']}"
    verify_ssl = bool(cfg.get("peplink_api_verify_ssl", False))
    return PeplinkAPI(base_url, client_id, client_secret, verify_ssl)
