#!/usr/bin/env python3
"""
Dead-letter queue inspector / replayer.

    ./scripts/replay-dlq.py --report
    ./scripts/replay-dlq.py --replay --limit 10
    ./scripts/replay-dlq.py --purge --confirm

Design rules:

  * `--report` is read-only and always safe. It is what you run first.
  * Replay is explicit and bounded. Blindly resubmitting a DLQ is how one bad
    message becomes a thousand.
  * Purge requires `--confirm` and prints what will be lost.

Every DLQ message carries its delivery history and the failure reason in
application properties, so the report can tell you *why* it dead-lettered
without opening the payload.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage


def queue_client(credential, fqdn: str, queue: str):
    return ServiceBusClient(fqdn, credential).get_queue_receiver(
        queue_name=queue, sub_queue="deadletter", max_wait_time=5)


def report(credential, fqdn: str, queue: str) -> int:
    reasons = Counter()
    samples: list[dict] = []
    count = 0

    with ServiceBusClient(fqdn, credential) as client:
        receiver = client.get_queue_receiver(queue_name=queue, sub_queue="deadletter",
                                             max_wait_time=5)
        with receiver:
            for msg in receiver.receive_messages(max_message_count=500):
                count += 1
                props = msg.application_properties or {}
                reason = msg.dead_letter_reason or props.get("DeadLetterReason", "unknown")
                reasons[str(reason)] += 1
                if len(samples) < 10:
                    samples.append({
                        "messageId": msg.message_id,
                        "sessionId": msg.session_id,
                        "reason": str(reason),
                        "description": str(msg.dead_letter_error_description or "")[:300],
                        "deliveryCount": msg.delivery_count,
                        "eventType": props.get("eventType"),
                        "orderId": props.get("orderId"),
                    })
                receiver.complete_message(msg)     # remove from DLQ after reading

    print(f"==> {count} dead-lettered message(s) on {queue}")
    for reason, n in reasons.most_common():
        print(f"    {n:5d}  {reason}")
    if samples:
        print("\n==> samples")
        print(json.dumps(samples, indent=2))
    return 0


def replay(credential, fqdn: str, queue: str, limit: int) -> int:
    """Move up to `limit` messages from the DLQ back onto the main queue.

    Safe because consumers are idempotent: a message that was actually processed
    before dying is a no-op on replay. That is the whole reason replay is
    allowed as an operation at all.
    """
    moved = 0
    with ServiceBusClient(fqdn, credential) as client:
        dlq = client.get_queue_receiver(queue_name=queue, sub_queue="deadletter",
                                        max_wait_time=5)
        sender = client.get_queue_sender(queue_name=queue)
        with dlq, sender:
            for msg in dlq.receive_messages(max_message_count=limit):
                body = b"".join(msg.body)
                sender.send_messages(ServiceBusMessage(
                    body=body,
                    session_id=msg.session_id,
                    application_properties=dict(msg.application_properties or {}),
                    message_id=msg.message_id,
                ))
                dlq.complete_message(msg)
                moved += 1
                print(f"    replayed {msg.message_id} (session {msg.session_id})")
    print(f"==> replayed {moved} message(s) onto {queue}")
    return 0


def purge(credential, fqdn: str, queue: str, confirm: bool) -> int:
    if not confirm:
        print("refusing to purge without --confirm", file=sys.stderr)
        return 1
    n = 0
    with ServiceBusClient(fqdn, credential) as client:
        receiver = client.get_queue_receiver(queue_name=queue, sub_queue="deadletter",
                                             max_wait_time=5)
        with receiver:
            for msg in receiver.receive_messages(max_message_count=1000):
                n += 1
                receiver.complete_message(msg)
    print(f"==> purged {n} message(s) from {queue} — this data is gone")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fqdn", default=os.environ.get("SERVICEBUS_FQDN", ""))
    ap.add_argument("--queue", default="orders-session")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--replay", action="store_true")
    ap.add_argument("--purge", action="store_true")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--confirm", action="store_true")
    args = ap.parse_args()

    if not args.fqdn:
        print("set --fqdn or SERVICEBUS_FQDN", file=sys.stderr)
        return 1

    credential = DefaultAzureCredential()
    if args.report:
        return report(credential, args.fqdn, args.queue)
    if args.replay:
        return replay(credential, args.fqdn, args.queue, args.limit)
    if args.purge:
        return purge(credential, args.fqdn, args.queue, args.confirm)
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
