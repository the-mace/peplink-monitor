"""Peplink local REST API client for WAN health status polling."""

import datetime
import json
import logging
import ssl
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger(__name__)


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

    def get_wan_status(self) -> list[dict[str, Any]]:
        """Return per-WAN status dicts from GET /api/status.wan.connection.

        Each dict contains: wan_id, name, status_led, message, uptime_seconds, enabled.
        WANs are returned in priority order as given by the API's 'order' key.
        """
        token = self._ensure_token()
        try:
            resp = self._do_request(
                "GET", f"/api/status.wan.connection?accessToken={token}"
            )
        except PeplinkAPIError as exc:
            # Token may have been invalidated server-side — retry once
            if "401" in str(exc):
                log.warning("Peplink API token rejected — re-authenticating")
                self._access_token = None
                token = self._ensure_token()
                resp = self._do_request(
                    "GET", f"/api/status.wan.connection?accessToken={token}"
                )
            else:
                raise

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


def from_config(cfg: dict) -> "PeplinkAPI | None":
    """Build a PeplinkAPI from config dict, or None if credentials are missing."""
    client_id = cfg.get("peplink_api_client_id", "").strip()
    client_secret = cfg.get("peplink_api_client_secret", "").strip()
    if not client_id or not client_secret:
        return None
    base_url = f"https://{cfg['host']}"
    verify_ssl = bool(cfg.get("peplink_api_verify_ssl", False))
    return PeplinkAPI(base_url, client_id, client_secret, verify_ssl)
