"""HTTP client for MediaMTX Control API v3."""

from __future__ import annotations

import logging
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
    ) -> tuple[bool, str | None]:
        """
        POST /v3/config/paths/add/{name}
        Returns (ok, error_message).
        """
        body: dict[str, Any] = {
            "name": name,
            "source": source,
            "rtspTransport": rtsp_transport,
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
