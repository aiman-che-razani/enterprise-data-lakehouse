#!/usr/bin/env python3
"""
Open-loop HTTP load test for the RetailNexus API.

Requests are issued on a fixed schedule (``--rps``) regardless of how fast earlier ones complete, so a slow
server shows up as latency (queueing) rather than silently lowering the offered load, which is what a
closed-loop tool does ("coordinated omission"). The ROADMAP criterion is "p99 < 200 ms under 100 RPS".

    python scripts/performance/api_load_test.py --base-url http://localhost:8000 \\
        --api-key dev-api-key-retailnexus-2026 --rps 100 --duration 60

Run the target with ``API_RATE_LIMIT_PER_MINUTE=0`` (a single key at 100 RPS would otherwise be throttled
with 429s, which this script reports separately and does not count as success).

Endpoints are discovered from ``/openapi.json``: every GET without required parameters, plus the
``{domain}`` / ``{table_name}`` routes filled from the data the API itself lists. Exit status is 1 when
the p99 target or the error budget is missed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from dataclasses import dataclass, field

import httpx


@dataclass
class Result:
    latencies_ms: list[float] = field(default_factory=list)
    by_status: dict[int, int] = field(default_factory=dict)
    by_path: dict[str, list[float]] = field(default_factory=dict)
    errors: int = 0


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(pct / 100 * len(ordered) + 0.5) - 1))
    return ordered[idx]


async def discover(client: httpx.AsyncClient) -> list[str]:
    spec = (await client.get("/openapi.json")).json()
    paths = [
        p
        for p, ops in spec["paths"].items()
        if "get" in ops and "{" not in p and not p.startswith(("/metrics", "/docs", "/redoc"))
    ]
    # fill the templated routes from what the API reports
    domains: list[str] = []
    r = await client.get("/pipeline/watermarks")
    if r.status_code == 200:
        body = r.json()
        rows = body if isinstance(body, list) else body.get("watermarks", body.get("items", []))
        domains = [x["domain"] for x in rows if isinstance(x, dict) and "domain" in x][:5]
    tables: list[str] = []
    r = await client.get("/catalog/tables")
    if r.status_code == 200:
        body = r.json()
        rows = body if isinstance(body, list) else body.get("tables", body.get("items", []))
        tables = [x["table_name"] for x in rows if isinstance(x, dict) and "table_name" in x][:5]
    paths += [f"/pipeline/watermarks/{d}" for d in domains]
    for t in tables:
        paths += [f"/catalog/tables/{t}", f"/catalog/columns/{t}", f"/catalog/lineage/{t}"]
    return paths


async def one(client: httpx.AsyncClient, path: str, res: Result) -> None:
    start = time.perf_counter()
    try:
        r = await client.get(path)
        status = r.status_code
    except httpx.HTTPError:
        res.errors += 1
        return
    ms = (time.perf_counter() - start) * 1000
    res.by_status[status] = res.by_status.get(status, 0) + 1
    if status == 200:
        res.latencies_ms.append(ms)
        res.by_path.setdefault(path, []).append(ms)


async def run(args: argparse.Namespace) -> int:
    limits = httpx.Limits(
        max_connections=args.connections, max_keepalive_connections=args.connections
    )
    headers = {"X-API-Key": args.api_key}
    async with httpx.AsyncClient(
        base_url=args.base_url, headers=headers, limits=limits, timeout=10.0
    ) as client:
        paths = await discover(client)
        print(f"endpoints under test ({len(paths)}): {', '.join(paths)}")

        warm = Result()
        for p in paths:  # warm connection pool, query plans and caches
            await one(client, p, warm)

        res = Result()
        total = int(args.rps * args.duration)
        interval = 1.0 / args.rps
        tasks: list[asyncio.Task[None]] = []
        t0 = time.perf_counter()
        for i in range(total):
            delay = t0 + i * interval - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(one(client, paths[i % len(paths)], res)))
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - t0

    ok = len(res.latencies_ms)
    p50, p95, p99 = (percentile(res.latencies_ms, q) for q in (50, 95, 99))
    print(f"\noffered {args.rps} RPS for {args.duration}s  ->  {total} requests in {elapsed:.1f}s")
    print(f"status codes: {dict(sorted(res.by_status.items()))}  transport errors: {res.errors}")
    print(
        f"latency (200s only, n={ok}): p50={p50:.1f} ms  p95={p95:.1f} ms  p99={p99:.1f} ms  "
        f"max={max(res.latencies_ms, default=float('nan')):.1f} ms  mean={statistics.fmean(res.latencies_ms):.1f} ms"
    )
    print("\nslowest endpoints by p99:")
    ranked = sorted(res.by_path.items(), key=lambda kv: -percentile(kv[1], 99))[:5]
    for path, vals in ranked:
        print(f"  {percentile(vals, 99):7.1f} ms  p99   {len(vals):5d} req  {path}")

    failures = total - ok
    summary = {
        "rps": args.rps,
        "duration_s": args.duration,
        "requests": total,
        "success": ok,
        "failed": failures,
        "p50_ms": round(p50, 1),
        "p95_ms": round(p95, 1),
        "p99_ms": round(p99, 1),
    }
    print("\n" + json.dumps(summary))
    passed = p99 <= args.p99_target_ms and failures <= total * args.max_error_rate
    print(
        f"{'PASS' if passed else 'FAIL'}: p99 target {args.p99_target_ms} ms, error budget {args.max_error_rate:.1%}"
    )
    return 0 if passed else 1


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--base-url", default="http://localhost:8000")
    p.add_argument("--api-key", required=True)
    p.add_argument("--rps", type=float, default=100.0)
    p.add_argument("--duration", type=float, default=60.0, help="seconds")
    p.add_argument("--connections", type=int, default=200)
    p.add_argument("--p99-target-ms", type=float, default=200.0)
    p.add_argument("--max-error-rate", type=float, default=0.001)
    return asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
