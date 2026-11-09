"""HTTP client for the documented simulator contract."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Iterable

import httpx


@dataclass
class SimulatorError(RuntimeError):
    status: int
    code: str
    detail: object

    def __str__(self) -> str:
        return f"{self.status} {self.code}: {self.detail}"


class SimulatorClient:
    """Small, retrying client for the simulator's HTTP contract.

    The simulator is local in the assignment, but the controller is expected
    to survive a transient server restart or a short connection hiccup.  We
    only retry transport failures and explicitly transient HTTP statuses;
    governance conflicts (409), validation errors and other 4xx responses
    remain deterministic decisions and are never replayed blindly.
    """

    def __init__(self, base_url: str, timeout: float = 120.0, retries: int = 3):
        self.client = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout)
        self.retries = max(0, int(retries))

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, path: str, **kwargs):
        response = None
        for attempt in range(self.retries + 1):
            try:
                response = self.client.request(method, path, **kwargs)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                if attempt >= self.retries:
                    raise SimulatorError(503, "transport_error", str(exc)) from exc
                time.sleep(0.2 * (2 ** attempt))
                continue
            if response.status_code not in (408, 425, 429) and response.status_code < 500:
                break
            if attempt >= self.retries:
                break
            response.close()
            time.sleep(0.2 * (2 ** attempt))
        assert response is not None
        if response.is_error:
            try:
                body = response.json()
            except ValueError:
                body = {"error": "http_error", "detail": response.text}
            raise SimulatorError(response.status_code, body.get("error", "http_error"), body.get("detail"))
        return response

    def get(self, path: str, params: dict | None = None):
        return self._request("GET", path, params=params).json()

    def post(self, path: str, body: dict):
        return self._request("POST", path, json=body).json()

    def put(self, path: str, body: dict):
        return self._request("PUT", path, json=body).json()

    def iter_chunk(self, chunk: int) -> Iterable[str]:
        yielded = False
        for attempt in range(self.retries + 1):
            try:
                with self.client.stream("GET", f"/events/chunks/{chunk}") as response:
                    if response.status_code in (408, 425, 429) or response.status_code >= 500:
                        if attempt < self.retries:
                            time.sleep(0.2 * (2 ** attempt))
                            continue
                    if response.is_error:
                        try:
                            body = response.json()
                        except ValueError:
                            body = {"error": "http_error", "detail": response.text}
                        raise SimulatorError(response.status_code, body.get("error", "http_error"),
                                             body.get("detail"))
                    for line in response.iter_lines():
                        yielded = True
                        yield line
                    return
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                # Once a line has reached the caller, replaying the whole
                # stream into the same transaction would duplicate its
                # in-memory aggregates. Let the transaction roll back and let
                # the outer run retry the complete chunk instead.
                if yielded:
                    raise SimulatorError(503, "chunk_stream_interrupted", str(exc)) from exc
                if attempt >= self.retries:
                    raise SimulatorError(503, "transport_error", str(exc)) from exc
                time.sleep(0.2 * (2 ** attempt))

    def state(self) -> dict:
        return self.get("/sim/state")

    def workflows(self) -> list[dict]:
        return self.get("/workflows")

    def config(self, workflow_id: str) -> dict:
        return self.get(f"/workflows/{workflow_id}/config")

    def chunks(self, kind: str, after: int = -1) -> list[dict]:
        return self.get("/events/chunks", {"kind": kind, "after": after})

    def advance(self, minutes: int) -> dict:
        return self.post("/sim/advance", {"minutes": minutes})

    def approvals(self) -> list[dict]:
        return self.get("/approvals")
