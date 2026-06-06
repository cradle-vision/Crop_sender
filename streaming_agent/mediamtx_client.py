"""HTTP client for MediaMTX Control API v3."""

from __future__ import annotations

import logging
import time
from typing import Any
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)


class MediaMTXClient:
    def __init__(self, api_base: str, timeout: float = 15.0):
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout

    def _url(self, path: str) -> str:
        return f"{self.api_base}{path}"

    def add_rtsp_path(
        self,
        name: str,
        source: str,
        rtsp_transport: str = "tcp",
        *,
        source_on_demand: bool = True,
        source_on_demand_close_after: str = "20s",
    ) -> tuple[bool, str | None]:
        """
        POST /v3/config/paths/add/{name}
        Returns (ok, error_message).
        """
        body: dict[str, Any] = {
            "name": name,
            "source": source,
            "rtspTransport": rtsp_transport,
            "sourceOnDemand": source_on_demand,
            "sourceOnDemandCloseAfter": source_on_demand_close_after,
        }
        try:
            r = requests.post(
                self._url(f"/v3/config/paths/add/{quote(name, safe='')}"),
                json=body,
                timeout=self.timeout,
            )
            if r.status_code == 200:
                return True, None
            err = self._err_body(r)
            return False, err
        except requests.RequestException as e:
            logger.exception("MediaMTX add path failed")
            return False, str(e)

    def patch_path(self, name: str, body: dict[str, Any]) -> tuple[bool, str | None]:
        """PATCH /v3/config/paths/patch/{name}"""
        try:
            r = requests.patch(
                self._url(f"/v3/config/paths/patch/{quote(name, safe='')}"),
                json=body,
                timeout=self.timeout,
            )
            if r.status_code == 200:
                return True, None
            return False, self._err_body(r)
        except requests.RequestException as e:
            logger.exception("MediaMTX patch path failed")
            return False, str(e)

    def ensure_relay_path(
        self,
        name: str,
        source: str,
        rtsp_transport: str = "tcp",
    ) -> tuple[bool, str | None]:
        """Idempotent relay path: patch if exists, else add."""
        existing = self._path_item(name)
        body = {
            "source": source,
            "rtspTransport": rtsp_transport,
            "sourceOnDemand": True,
            "sourceOnDemandCloseAfter": "20s",
        }
        if existing is not None:
            ok, err = self.patch_path(name, body)
            if ok:
                return True, None
            logger.warning("relay patch failed path=%s err=%s — recreate", name, err)
            self.delete_path(name)

        return self.add_rtsp_path(
            name,
            source,
            rtsp_transport,
            source_on_demand=True,
            source_on_demand_close_after="20s",
        )

    def wait_path_ready(self, name: str, timeout_sec: float = 12.0) -> bool:
        """Poll until MediaMTX reports a ready source for the path."""
        deadline = time.monotonic() + max(1.0, timeout_sec)
        while time.monotonic() < deadline:
            item = self._path_item(name)
            if item:
                ready = item.get("ready")
                readers = item.get("readers") or []
                if ready or readers:
                    return True
            time.sleep(0.4)
        return False

    def _path_item(self, name: str) -> dict[str, Any] | None:
        data = self.list_paths()
        if not data:
            return None
        items = data.get("items") or []
        for item in items:
            if isinstance(item, dict) and item.get("name") == name:
                return item
        return None

    def delete_path(self, name: str) -> tuple[bool, str | None]:
        """DELETE /v3/config/paths/delete/{name}"""
        try:
            r = requests.delete(
                self._url(f"/v3/config/paths/delete/{quote(name, safe='')}"),
                timeout=self.timeout,
            )
            if r.status_code == 200:
                return True, None
            if r.status_code == 404:
                return True, None  # idempotent
            return False, self._err_body(r)
        except requests.RequestException as e:
            logger.exception("MediaMTX delete path failed")
            return False, str(e)

    def list_paths(self) -> dict[str, Any] | None:
        """GET /v3/paths/list"""
        try:
            r = requests.get(self._url("/v3/paths/list"), timeout=self.timeout)
            if r.status_code != 200:
                logger.warning("paths/list: %s %s", r.status_code, r.text[:300])
                return None
            return r.json()
        except requests.RequestException as e:
            logger.warning("paths/list failed: %s", e)
            return None

    @staticmethod
    def _err_body(r: requests.Response) -> str:
        try:
            j = r.json()
            if isinstance(j, dict) and "error" in j:
                return str(j["error"])
        except Exception:
            pass
        return f"{r.status_code} {r.text[:500]}"
