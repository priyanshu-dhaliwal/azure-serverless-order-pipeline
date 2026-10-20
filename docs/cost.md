# Cost model — Serverless Order Pipeline

Estimates in **INR/month**, `centralindia`, list prices at time of writing.

## Scenario A — 10,000 orders/month (~330/day, ~0.004/s)

| Resource | Config | Monthly |
|---|---|---|
| Cosmos DB | Serverless, ~40k RU + 1 GB | ~₹2,000 |
| Service Bus | Standard, ~30k operations | ~₹350 |
| Functions | Flex Consumption, ~200k executions, 256 MB | ~₹450 |
| Storage | Standard LRS, 5 GB | ~₹120 |
| App Insights | 1 GB ingestion | ~₹250 |
| Log Analytics | 2 GB | ~₹600 |
| **Total** | | **~₹3,800** |

At this volume the pipeline is almost free. That is the point of serverless:
the fixed cost of Service Bus Premium (~₹7,200) would be 2× the entire bill.

## Scenario B — 1,000,000 orders/month (~0.4/s, peaks of 20/s)

| Resource | Config | Monthly |
|---|---|---|
| Cosmos DB | Autoscale 1k–10k RU/s, ~30 GB | ~₹18,000 |
| Service Bus | Premium, 1 MU | ~₹7,200 |
| Functions | Flex, ~30M executions, 512 MB | ~₹8,500 |
| Storage | 50 GB | ~₹900 |
| App Insights | 25 GB, 20% sampling | ~₹4,000 |
| Log Analytics | 15 GB | ~₹4,500 |
| **Total** | | **~₹43,000** |

## Scenario C — 20,000,000 orders/month (~7.7/s, peaks of 150/s)

| Resource | Config | Monthly |
|---|---|---|
| Cosmos DB | Autoscale 5k–50k RU/s, 300 GB | ~₹95,000 |
| Service Bus | Premium 4 MU | ~₹28,800 |
| Functions | Flex, ~600M executions | ~₹42,000 |
| Storage | 400 GB | ~₹6,000 |
| App Insights | 200 GB with adaptive sampling | ~₹18,000 |
| Log Analytics | 120 GB (Basic tier for non-security) | ~₹9,000 |
| **Total** | | **~₹198,000** |

That is ~₹0.0099 per order, or about one paisa. The interesting thing is not the
total — it is the *shape*: at scale, Cosmos RU and Function executions dominate,
and both are proportional to order volume. There is no step change, which is why
this design scales without a re-architecture until roughly 50M orders/month.

## Where to cut, and what you lose

1. **Cosmos DB autoscale → manual provisioned RU.** Saves ~30% if you can
   predict load. You lose the ability to absorb a flash sale without throttling.
2. **Service Bus Standard instead of Premium** below ~1k msg/s. Saves ₹6,850.
   You lose sessions on some tiers (sessions require Standard or above — check
   your tier), private endpoints, and the 1 MB message limit drops to 256 KB.
3. **App Insights sampling to 5%.** Saves ₹3,000 at scenario B. You lose the
   ability to reconstruct a single order's trace for support — which is the main
   reason it is there. Use 20% and adaptive sampling instead.
4. **Basic log tier for outbox traces.** Saves ~₹3,000. Basic logs cannot be
   queried with alert rules, so the `orders-stuck` alert must stay on the
   Analytics tier.

## The cost of the failure modes

| Failure | Cost without the pattern | Cost with it |
|---|---|---|
| Double charge on duplicate delivery | refund + support ticket + chargeback (~₹2,500/incident) | 0 |
| Order lost by dual-write bug | lost sale + customer trust | 0 |
| Poison message looping | Function execution cost grows linearly with retry time | 5 retries, then DLQ |

At 1M orders/month, a 0.1% duplicate rate is 1,000 double charges. The
idempotency machinery that prevents them costs ~₹0 (it is code, not
infrastructure).
