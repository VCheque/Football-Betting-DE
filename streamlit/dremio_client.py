"""Dremio REST API client for the Streamlit serving layer.

Provides a simple `query(sql) -> pd.DataFrame` interface backed by the
Dremio Jobs REST API.  Authentication token is obtained once per session
and reused until a 401 is returned, at which point it re-authenticates.

Configuration via environment variables (all have sensible defaults for local
Docker Compose development):

    DREMIO_HOST      — hostname of the Dremio container (default: dremio)
    DREMIO_PORT      — Dremio REST API port (default: 9047)
    DREMIO_USER      — Dremio username (default: admin)
    DREMIO_PASSWORD  — Dremio password (default: admin12345)
    DREMIO_TIMEOUT   — per-request timeout in seconds (default: 60)
"""

from __future__ import annotations

import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd


class DremioClient:
    """Thin REST client for running SQL against Dremio and returning DataFrames."""

    _PAGE_SIZE = 500  # rows per result page

    def __init__(
        self,
        host: str | None = None,
        port: str | int | None = None,
        user: str | None = None,
        password: str | None = None,
        timeout: int | None = None,
    ) -> None:
        self._host = host or os.getenv("DREMIO_HOST", "dremio")
        self._port = port or os.getenv("DREMIO_PORT", "9047")
        self._user = user or os.getenv("DREMIO_USER", "admin")
        self._password = password or os.getenv("DREMIO_PASSWORD", "admin12345")
        self._timeout = int(timeout or os.getenv("DREMIO_TIMEOUT", "60"))
        self._base = f"http://{self._host}:{self._port}"
        self._token: str | None = None

    # ------------------------------------------------------------------
    # Low-level HTTP helpers
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
        token: str | None = None,
    ) -> dict | None:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        req = Request(self._base + path, data=data, headers=headers, method=method)
        with urlopen(req, timeout=self._timeout) as resp:  # noqa: S310
            content = resp.read()
            return json.loads(content) if content else None

    def _auth(self) -> str:
        resp = self._request(
            "POST",
            "/apiv2/login",
            payload={"userName": self._user, "password": self._password},
        )
        assert resp is not None
        return resp["token"]

    def _token_headers(self) -> str:
        if self._token is None:
            self._token = self._auth()
        return self._token

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------

    def _submit_job(self, sql: str, token: str) -> str:
        resp = self._request(
            "POST",
            "/api/v3/sql",
            payload={"sql": sql, "context": []},
            token=token,
        )
        assert resp is not None
        return resp["id"]

    def _wait_for_job(self, job_id: str, token: str, poll_interval: float = 0.5) -> None:
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            resp = self._request("GET", f"/api/v3/job/{job_id}", token=token)
            assert resp is not None
            state = resp.get("jobState", "")
            if state == "COMPLETED":
                return
            if state in ("FAILED", "CANCELED", "ENQUEUED_FAILED"):
                error = resp.get("errorMessage", "unknown error")
                raise RuntimeError(f"Dremio job {job_id} failed: {error}")
            time.sleep(poll_interval)
        raise TimeoutError(f"Dremio job {job_id} did not complete within {self._timeout}s")

    def _fetch_results(self, job_id: str, token: str) -> list[dict]:
        rows: list[dict] = []
        offset = 0
        while True:
            resp = self._request(
                "GET",
                f"/api/v3/job/{job_id}/results?offset={offset}&limit={self._PAGE_SIZE}",
                token=token,
            )
            assert resp is not None
            batch = resp.get("rows", [])
            rows.extend(batch)
            if len(batch) < self._PAGE_SIZE:
                break
            offset += self._PAGE_SIZE
        return rows

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL on Dremio and return the result as a pandas DataFrame.

        Re-authenticates automatically on token expiry (HTTP 401).
        """
        for attempt in range(2):
            token = self._token_headers()
            try:
                job_id = self._submit_job(sql, token)
                self._wait_for_job(job_id, token)
                rows = self._fetch_results(job_id, token)
                return pd.DataFrame(rows)
            except HTTPError as exc:
                if exc.code == 401 and attempt == 0:
                    # Token expired — re-authenticate and retry once
                    self._token = None
                    continue
                raise
        raise RuntimeError("Authentication failed after re-try")


# Module-level singleton — one client per Streamlit worker process
_client: DremioClient | None = None


def get_client() -> DremioClient:
    """Return the shared DremioClient instance (lazy init)."""
    global _client
    if _client is None:
        _client = DremioClient()
    return _client


def query(sql: str) -> pd.DataFrame:
    """Convenience wrapper: execute SQL and return a DataFrame."""
    return get_client().query(sql)
