# The saga, state by state

Every order is a saga. This document is the complete contract: states,
transitions, who may cause each one, the event that follows, and the
compensation if it goes wrong.

## States

| State | Meaning | Terminal | Who moves it |
|---|---|---|---|
| `CREATED` | order persisted, outbox event written | no | `order_submit` |
| `STOCK_RESERVED` | inventory holds the SKUs | no | `step_worker` |
| `PAYMENT_AUTHORISED` | payment provider authorised the total | no | `step_worker` |
| `CONFIRMED` | both steps succeeded | **yes** | `saga_advance` |
| `COMPENSATING` | undoing what was done | no | `step_worker` / `compensation_worker` |
| `CANCELLED` | compensations completed | **yes** | `compensation_worker` |
| `REJECTED` | failed validation before any side effect | **yes** | `order_submit` |
| `STUCK` | compensation failed; a human must act | no | `compensation_worker` |

## Transitions

```
CREATED ──────────────▶ STOCK_RESERVED ──────────▶ PAYMENT_AUTHORISED ──────▶ CONFIRMED
   │                          │                             │
   │                          └── stock/payment failure ─────┤
   │                                                        │
   └── validation failure ──▶ REJECTED                       │
                   │                                        │
                   └────────────▶ COMPENSATING ◀────────────┘
                                      │
                                      ├──▶ CANCELLED   (compensations ok)
                                      └──▶ STUCK       (compensation failed)
```

### Table

| From | To | Trigger | Guard | Side effects |
|---|---|---|---|---|
| `CREATED` | `STOCK_RESERVED` | `StockReserveRequested` | state is `CREATED` | inventory reserve (idempotent on orderId); outbox `PaymentRequested` |
| `CREATED` | `COMPENSATING` | stock failure | — | outbox `Compensate` |
| `STOCK_RESERVED` | `PAYMENT_AUTHORISED` | `PaymentRequested` | state is `STOCK_RESERVED` | provider charge (idempotency key); outbox `ConfirmRequested` |
| `STOCK_RESERVED` | `COMPENSATING` | payment failure | — | outbox `Compensate` |
| `PAYMENT_AUTHORISED` | `CONFIRMED` | `ConfirmRequested` | state is `PAYMENT_AUTHORISED` | — |
| `COMPENSATING` | `CANCELLED` | `Compensate` | state is `COMPENSATING` | release stock, refund (both idempotent) |
| `COMPENSATING` | `STUCK` | compensation error | — | none — raises for dead-letter |

Any other pair raises `IllegalTransition`. That is a *feature*: it is how a
duplicate or replayed message is refused without side effects.

## Compensation matrix

| Completed step | Compensation | Idempotency key | Twice is safe? |
|---|---|---|---|
| stock reserved | `release(order_id)` | `order_id` | yes — releasing a released reservation is a no-op |
| payment authorised | `refund(authorisation_id)` | `authorisation_id` | yes — the provider deduplicates on it |

**Refund is the dangerous one.** It is keyed on the authorisation id, not the
order id, because an order could in principle be re-authorised after a failed
compensation. Keying on the authorisation means the refund can only ever be
applied once *per authorisation*.

## What is deliberately absent

**No `REFUNDED` state.** Refunding is a compensation step, not a lifecycle
state. Modelling it as a state invites the question "what happens if we refund
twice?", which the answer above already settles.

**No partial confirmation.** An order is all-or-nothing. Partial fulfilment is a
different domain (split shipments), and pretending a saga can express it is how
you get an order stuck between `PAYMENT_AUTHORISED` and `CONFIRMED` forever.

**No timeout-based cancellation.** An order that never gets its events is a
stuck order, and a timer that cancels it would race the delivery. `STUCK` plus an
alert is honest; a timer is optimistic.
