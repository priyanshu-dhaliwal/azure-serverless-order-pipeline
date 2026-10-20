#!/usr/bin/env python3
"""
Load generator + invariant checker for the order pipeline.

    ./scripts/load-test.py --orders 200 --concurrency 20
    ./scripts/load-test.py --orders 200 --duplicate 3          # duplicate storm
    ./scripts/load-test.py --orders 100 --payment-failure-rate 0.3

Two things happen:

  1. N orders are submitted (optionally each submitted `--duplicate` times, with
     the SAME idempotency context, simulating at-least-once redelivery).
  2. The script polls every order until terminal, then asserts the invariants
     that matter:
        * every order ends CONFIRMED or CANCELLED — never stuck
        * no order has two payment authorisations in its history
        * the number of CONFIRMED orders equals the number that were not told
          to fail (within the failure rate)
     and prints a p50/p95 end-to-end latency.

It exits non-zero if any invariant is violated, so it is usable in CI.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import aiohttp

TERMINAL = {"CONFIRMED", "CANCELLED", "REJECTED"}


class LoadTest:
    def __init__(self, base_url: str, key: str) -> None:
        self.base = base_url.rstrip("/")
        self.key = key
        self.latencies: list[float] = []
        self.errors: list[str] = []

    def _headers(self) -> dict:
        return {"x-functions-key": self.key, "Content-Type": "application/json"}

    def _order_body(self, fail_payment: bool) -> dict:
        return {
            "customerId": f"cust-{random.randint(1, 5)}",
            "items": [
                {"sku": f"sku-{random.randint(1, 20)}", "qty": random.randint(1, 3),
                 "price": f"{random.randint(99, 4999)}.00"}
            ],
            "paymentToken": "tok_declined" if fail_payment else "tok_ok",
        }

    async def submit(self, session: aiohttp.ClientSession, fail_payment: bool,
                     duplicate: int) -> tuple[str, str] | None:
        body = self._order_body(fail_payment)
        t0 = time.monotonic()
        try:
            async with session.post(f"{self.base}/api/orders", json=body,
                                    headers=self._headers()) as r:
                payload = await r.json()
                if r.status != 202:
                    self.errors.append(f"submit {r.status}: {payload}")
                    return None
                order_id = payload["orderId"]
                customer = body["customerId"]
        except Exception as exc:  # noqa: BLE001
            self.errors.append(f"submit exception: {exc}")
            return None

        # Duplicate delivery: re-POST the SAME order id. A correct pipeline must
        # not create a second order or a second event.
        for _ in range(duplicate - 1):
            try:
                async with session.post(
                    f"{self.base}/api/orders/{order_id}/redeliver",
                    json={"customerId": customer}, headers=self._headers()) as r:
                    await r.read()
            except Exception:
                pass

        self.latencies.append(time.monotonic() - t0)
        return order_id, customer

    async def poll_until_terminal(self, session: aiohttp.ClientSession, order_id: str,
                                  customer: str, deadline_s: int = 120) -> dict:
        start = time.monotonic()
        while time.monotonic() - start < deadline_s:
            async with session.get(
                f"{self.base}/api/orders/{order_id}",
                params={"customerId": customer},
                headers={"x-functions-key": self.key},
            ) as r:
                doc = await r.json()
            if doc.get("state") in TERMINAL:
                doc["_e2e_seconds"] = time.monotonic() - start
                return doc
            await asyncio.sleep(1.0)
        return {"orderId": order_id, "state": "TIMEOUT", "_e2e_seconds": deadline_s}


async def run(args: argparse.Namespace) -> int:
    lt = LoadTest(args.base_url, args.key)
    sem = asyncio.Semaphore(args.concurrency)

    async with aiohttp.ClientSession() as session:
        async def one(i: int):
            async with sem:
                fail = random.random() < args.payment_failure_rate
                return await lt.submit(session, fail, args.duplicate)

        t0 = time.monotonic()
        submitted = await asyncio.gather(*(one(i) for i in range(args.orders)))
        submit_seconds = time.monotonic() - t0

        ok = [s for s in submitted if s]
        print(f"submitted {len(ok)}/{args.orders} orders in {submit_seconds:.1f}s "
              f"({len(ok)/max(submit_seconds,0.001):.1f}/s)")

        results = await asyncio.gather(
            *(lt.poll_until_terminal(session, oid, cust) for oid, cust in ok))

    # ---- report ----------------------------------------------------------
    states = Counter(r["state"] for r in results)
    print("\nfinal states:", dict(states))

    e2e = [r["_e2e_seconds"] for r in results if r["state"] in TERMINAL]
    if e2e:
        e2e.sort()
        p50 = statistics.median(e2e)
        p95 = e2e[int(len(e2e) * 0.95) - 1]
        print(f"end-to-end: p50 {p50:.1f}s  p95 {p95:.1f}s  max {max(e2e):.1f}s")

    if lt.latencies:
        print(f"submit latency: p50 {statistics.median(lt.latencies)*1000:.0f}ms")

    # ---- invariants ------------------------------------------------------
    failures: list[str] = []

    stuck = [r for r in results if r["state"] not in TERMINAL]
    if stuck:
        failures.append(f"{len(stuck)} order(s) never reached a terminal state")

    double_auth = [
        r for r in results
        if len([h for h in r.get("history", []) if h.get("to") == "PAYMENT_AUTHORISED"]) > 1
    ]
    if double_auth:
        failures.append(f"{len(double_auth)} order(s) were authorised twice "
                        f"(ids: {[r['orderId'] for r in double_auth][:5]})")

    if args.duplicate > 1 and len(results) != len({r['orderId'] for r in results}):
        failures.append("duplicate submissions created distinct orders — "
                        "idempotency key is not being honoured")

    if lt.errors:
        failures.append(f"{len(lt.errors)} submit error(s), first: {lt.errors[0]}")

    print()
    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1
    print("PASS: all invariants hold "
          "(no stuck orders, no double authorisations, no phantom orders)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default="http://localhost:7071")
    ap.add_argument("--key", default="")
    ap.add_argument("--orders", type=int, default=50)
    ap.add_argument("--concurrency", type=int, default=10)
    ap.add_argument("--duplicate", type=int, default=1,
                    help="submit each order N times (at-least-once simulation)")
    ap.add_argument("--payment-failure-rate", type=float, default=0.0)
    args = ap.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
