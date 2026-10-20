# Failure drills — what was broken on purpose

Each drill states what was broken, what the correct behaviour is, and what was
observed. A pipeline that has never been broken is a pipeline you do not know.

## D1 — Duplicate delivery storm

**Broken:** every message published 3 times (Service Bus redelivery + duplicate
send), 100 orders.

**Correct behaviour:** 100 orders, exactly 100 `CONFIRMED`, zero double charges,
zero duplicate `PAYMENT_AUTHORISED` history entries.

**Mechanism under test:** etag-guarded transitions + payment idempotency key +
`requiresDuplicateDetection` on the queue.

**Observed:**

```
submitted 100/100 orders in 4.2s (23.8/s)
final states: {'CONFIRMED': 100}
end-to-end: p50 6.1s  p95 11.4s  max 14.0s
PASS: all invariants hold (no stuck orders, no double authorisations, no phantom orders)
```

**Note:** the duplicate sends pushed `outbox_published` counts to ~3× the order
count, and every extra publish completed as a no-op at the consumer. That is the
expected cost of at-least-once delivery, and it is cheaper than exactly-once.

## D2 — Payment provider rejecting 30% of charges

**Broken:** `PAYMENT_FAILURE_RATE=0.3`, 100 orders.

**Correct behaviour:** ~70 `CONFIRMED`, ~30 `CANCELLED`, every cancelled order
with stock released, and **no** order left in `COMPENSATING`.

**Mechanism under test:** saga compensation path.

**Observed:**

```
final states: {'CONFIRMED': 71, 'CANCELLED': 29}
end-to-end: p50 7.9s  p95 13.8s  max 17.2s
PASS
```

## D3 — Function host killed mid-saga

**Broken:** `az functionapp stop` during a 200-order run, restarted 90 seconds
later.

**Correct behaviour:** no order lost, no order stuck; messages redelivered after
`lockDuration` expired (60s) and completed normally.

**Mechanism under test:** at-least-once redelivery + idempotent handlers.

**Observed:**

```
submitted 200/200 in 9.1s
(final states after restart) {'CONFIRMED': 143, 'CANCELLED': 57}
end-to-end: p50 41s  p95 96s  max 118s
PASS — 0 stuck, 0 double authorisations
```

**What this proved:** the end-to-end latency budget for an order must be larger
than `lockDuration` × `maxDeliveryCount`. With 1-minute locks and 5 attempts, an
order can legitimately take ~5 minutes to recover after a host failure.

## D4 — Poison message

**Broken:** published a message with `eventType: "UnknownThing"` onto
`orders-session`.

**Correct behaviour:** 5 delivery attempts, then dead-letter; nothing silently
swallowed; the DLQ alert fires; `replay-dlq.py --report` shows the reason.

**Observed:**

```
==> 1 dead-lettered message(s) on orders-session
        1  MaxDeliveryCountExceeded
samples: [{"messageId":"...","reason":"MaxDeliveryCountExceeded",
           "eventType":"UnknownThing","deliveryCount":5}]
```

and the `orders-session` dead-letter metric alert fired within 5 minutes.

## D5 — Out-of-order message

**Broken:** `PaymentRequested` published for an order with no stock reservation
(bypassing the session queue, simulating a manual replay mistake).

**Correct behaviour:** rejected by the state machine (`IllegalTransition`), logged
as `step_noop`, message completed, order state unchanged.

**Observed:** one `step_noop` trace with `reason="illegal transition from current
state"`, no state change, no dead-letter. The message was *stale*, not *poison*,
so completing it is correct.

## What these drills changed

- D3 added the `maxAutoLockRenewalDuration` + explicit `lockDuration` in Bicep;
  the defaults (5 min) were longer than the function timeout, which made a
  stuck-session diagnosis much slower than it needed to be.
- D4 added `deadLetteringOnMessageExpiration` — messages were expiring instead
  of dead-lettering, which meant a silent drop with no alert.
- D5 added the `docType` guard in the change-feed publisher so lease documents
  are never mistaken for outbox events.
