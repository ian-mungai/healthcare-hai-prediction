"""Replay a fixed list of hospital-windows against the model API and report the split and the prediction comparison.

This is the canary proof: replayed requests, not live traffic. No caller uses the API yet, so the replay shows that the split is deterministic and
close to its share and how the two models' predictions differ on the same inputs. It cannot show how the canary behaves under real use. The key comes
from the environment variable ``MODEL_API_KEY`` and is never printed or written. A rate limit answer is waited out, and the whole replay has a
deadline that clamps every wait and every request timeout. A request timeout covers the whole HTTP exchange.
A result that arrives after the deadline is refused.

    MODEL_API_KEY=... python -m scripts.serving.replay --url http://127.0.0.1:8000 --hai-type HAI_1 --corpus hospital_windows.csv --output report.json

The corpus is a text file with one ``ccn,window_year`` line for each hospital-window.
"""

from __future__ import annotations

import argparse
import contextlib
import http.client
import json
import math
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
from collections import Counter
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from scripts.serving import compare

MAX_CORPUS = 20_000
MAX_RETRIES = 5
REQUEST_SECONDS = 5.0
DEFAULT_WAIT_SECONDS = 1.0
LINE = re.compile(r"([A-Za-z0-9]{6}),([0-9]{4})")
Send = Callable[[dict[str, Any], float], tuple[int, dict[str, str], dict[str, Any]]]


class ReplayDeadline(RuntimeError):
    """The replay did not finish within its deadline."""


def parse_corpus(text: str) -> list[tuple[str, int]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines or len(lines) > MAX_CORPUS:
        raise ValueError(f"the corpus must hold 1 to {MAX_CORPUS} lines")
    found = []
    for number, line in enumerate(lines, start=1):
        match = LINE.fullmatch(line)
        if match is None:
            raise ValueError(f"corpus line {number} is not 'ccn,window_year'")
        found.append((match.group(1), int(match.group(2))))
    return found


def run(
    send: Send, corpus: Sequence[tuple[str, int]], hai_type: str, *, clock: Callable[[], float], sleep: Callable[[float], None], deadline_seconds: float
) -> dict[str, Any]:
    """Send every hospital-window once, waiting out rate limit answers, and return the status counts, the split and the comparison."""
    deadline = clock() + deadline_seconds

    def remaining() -> float:
        left = deadline - clock()
        if left <= 0:
            raise ReplayDeadline(f"the replay did not finish within {deadline_seconds:g} s")
        return left

    statuses: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for ccn, year in corpus:
        payload = {"hai_type": hai_type, "ccn": ccn, "window_year": year}
        for attempt in range(MAX_RETRIES + 1):
            status, headers, body = send(payload, min(REQUEST_SECONDS, remaining()))
            if clock() > deadline:
                raise ReplayDeadline(f"the replay did not finish within {deadline_seconds:g} s")
            if status != 429 or attempt == MAX_RETRIES:
                break
            sleep(min(retry_after(headers), remaining()))
        statuses[str(status)] += 1
        if status == 200:
            rows.append({"served_by": body["served_by"], "prediction": body["prediction"], "model_version": body["model_version"]})
    return {
        "hai_type": hai_type,
        "requests": len(corpus),
        "statuses": dict(statuses),
        "served_by": dict(Counter(row["served_by"] for row in rows)),
        "model_versions": dict(Counter(row["model_version"] for row in rows)),
        "comparison": compare.compare(rows),
    }


def retry_after(headers: dict[str, str]) -> float:
    """The seconds the server asks for, or a short default when the header is absent, negative, not a number or not finite."""
    try:
        seconds = float(headers.get("Retry-After", DEFAULT_WAIT_SECONDS))
    except ValueError:
        return DEFAULT_WAIT_SECONDS
    return seconds if math.isfinite(seconds) and seconds >= 0 else DEFAULT_WAIT_SECONDS


def http_send(url: str, key: str) -> Send:
    parts = urllib.parse.urlsplit(url)
    if parts.scheme != "http" or not parts.hostname:
        raise ValueError("the URL must be an http address, normally the local end of the port forward")

    def send(payload: dict[str, Any], timeout: float) -> tuple[int, dict[str, str], dict[str, Any]]:
        """One deadline covers connection, headers and body. A worker thread does the exchange and the caller stops waiting at the deadline."""
        connection = http.client.HTTPConnection(parts.hostname or "", parts.port or 80, timeout=timeout)
        box: dict[str, Any] = {}

        def exchange() -> None:
            try:
                connection.request("POST", "/v1/predict", body=json.dumps(payload), headers={"Content-Type": "application/json", "X-API-Key": key})
                response = connection.getresponse()
                box["result"] = response.status, dict(response.getheaders()), json.loads(response.read() or b"{}")
            except (OSError, http.client.HTTPException, ValueError) as error:
                box["error"] = error

        worker = threading.Thread(target=exchange, daemon=True)
        worker.start()
        worker.join(timeout)
        if worker.is_alive():
            sock = connection.sock
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.shutdown(socket.SHUT_RDWR)
            connection.close()
            raise TimeoutError(f"the request did not finish within {timeout:g} s")
        connection.close()
        if "error" in box:
            raise box["error"]
        result: tuple[int, dict[str, str], dict[str, Any]] = box["result"]
        return result

    return send


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--hai-type", required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deadline-seconds", type=float, default=900.0)
    args = parser.parse_args()
    key = os.environ.get("MODEL_API_KEY", "")
    if not key:
        sys.stderr.write("MODEL_API_KEY is not set\n")
        return 2
    try:
        corpus = parse_corpus(args.corpus.read_text())
        report = run(http_send(args.url, key), corpus, args.hai_type, clock=time.monotonic, sleep=time.sleep, deadline_seconds=args.deadline_seconds)
    except (ValueError, OSError, ReplayDeadline) as error:
        sys.stderr.write(f"replay stopped: {type(error).__name__}: {error}\n")
        return 1
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    sys.stdout.write(
        json.dumps(
            {"requests": report["requests"], "statuses": report["statuses"], "served_by": report["served_by"], "comparison": report["comparison"]["status"]}
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
