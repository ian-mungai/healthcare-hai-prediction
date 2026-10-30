"""The image part of the model API cases: the real FastAPI app under uvicorn, driven over HTTP on the loopback address.

Run in the serving image with the repository mounted read-only at its working folder (the coordinating agent's heavy lane)::

    python -B -m scripts.serving.api_image_cases --output /tmp/serving_image_cases

The service behind the app is the stand-in world of ``serving_cases`` (Redis stand-in, stub models, memory sink), so these cases prove the adapter only: the
route, the headers, the body limit and the OpenAPI document. Not run in the agent sandbox.
"""

from __future__ import annotations

import argparse
import http.client
import importlib
import json
import socket
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.features.feast_harness import Checker, run_cases
from scripts.serving import serving_cases as base

STARTUP_SECONDS = 10.0
REQUEST_SECONDS = 5.0


def call(port: int, method: str, path: str, body: bytes | None = None, headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], Any]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=REQUEST_SECONDS)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        raw = response.read()
        return response.status, dict(response.getheaders()), json.loads(raw) if raw else None
    finally:
        connection.close()


def post(port: int, payload: dict[str, Any], key: str | None = base.KEY_A) -> tuple[int, dict[str, str], Any]:
    headers = {"Content-Type": "application/json", **({"X-API-Key": key} if key else {})}
    return call(port, "POST", "/v1/predict", json.dumps(payload).encode(), headers)


def case_http(c: Checker) -> None:
    """200, 401, 404, 422, 413, 429, health and the OpenAPI document through the real app."""
    from scripts.serving.api import create_app

    uvicorn = importlib.import_module("uvicorn")
    with base.captured_logs() as logs, base.world(rate=60, burst=4) as w:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(create_app(w.api), host="127.0.0.1", port=port, access_log=False, log_level="warning"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + STARTUP_SECONDS
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.05)
        c.check("the server started within its deadline", server.started)
        try:
            good = {"hai_type": base.HAI, "ccn": "AAA001", "window_year": 2022}
            status, headers, body = post(port, good)
            c.check("a valid request answers 200 with the request id header", status == 200 and headers.get("X-Request-ID") == body["request_id"])
            c.check("no key answers 401", post(port, good, None)[0] == 401)
            c.check("a wrong key answers 401", post(port, good, "sk-test-wrong-key-12345")[0] == 401)
            c.check("a malformed ccn answers 422", post(port, good | {"ccn": "AAA"})[0] == 422)
            c.check("an unknown hospital answers 404", post(port, good | {"ccn": "ZZZ999"})[0] == 404)
            oversize = call(port, "POST", "/v1/predict", b"{" + b" " * 2048 + b"}", {"Content-Type": "application/json", "X-API-Key": base.KEY_A})
            c.check("a body over the limit answers 413", oversize[0] == 413)
            c.check("health answers 200", call(port, "GET", "/v1/health")[0] == 200)
            limited = [post(port, good)[0] for _ in range(6)]
            c.check("the burst ends in 429", 429 in limited)
            document = call(port, "GET", "/v1/openapi.json")[2]
            c.check(
                "the OpenAPI document describes the predict route and its request", "/v1/predict" in document["paths"] and "hai_type" in json.dumps(document)
            )
            c.check("no key reached a log line", base.KEY_A not in logs.getvalue())
        finally:
            server.should_exit = True
            thread.join(timeout=10)
        c.check("the server stopped within its deadline", not thread.is_alive())


CASES: dict[str, tuple[Callable[[Checker], None], str]] = {"http": (case_http, "api")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    report = run_cases(CASES, [])
    (args.output / "report.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    for result in report["cases"]:
        for name in result["failed"]:
            sys.stderr.write(f"FAIL {result['case']}: {name}\n")
    return int(report["verdict"] != "pass")


if __name__ == "__main__":
    raise SystemExit(main())
