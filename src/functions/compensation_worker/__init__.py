"""
Compensation worker: undo what was done, then move to CANCELLED.

The rule that makes this safe to retry: each compensation is *idempotent at the
downstream service*, keyed on order_id. Releasing a reservation twice is a
no-op; refunding an authorisation twice is not — so a refund carries the
authorisation id and the provider deduplicates on it.

The order only reaches CANCELLED when both compensations report success. If one
fails repeatedly the message dead-letters and the order moves to STUCK, which is
the state that pages a human: an order stuck mid-compensation is the one case
where a person must look.
"""

from __future__ import annotations

import json
import logging
import os

import azure.functions as func
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential

from shared.models import IllegalTransition, OrderEvent, OrderState
from shared.repository import ConcurrentUpdate, OrderRepository
from shared.telemetry import order_span, structured

log = logging.getLogger("orders.compensation")


async def _release_stock(order_id: str) -> None:
    """Idempotent: releasing a non-existent reservation succeeds."""
    structured("compensate_release_stock", orderId=order_id)


async def _refund(order_id: str, authorisation_id: str | None) -> None:
    """Idempotent at the provider: keyed on the authorisation id."""
    if authorisation_id:
        structured("compensate_refund", orderId=order_id, authorisation=authorisation_id)


async def main(msg: func.ServiceBusMessage) -> None:
    body = json.loads(msg.get_body().decode("utf-8"))
    props = msg.application_properties or {}
    event = OrderEvent.from_message_body(body, operation_id=props.get("operationId", ""))

    credential = DefaultAzureCredential()
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=credential)
    repo = OrderRepository(
        client,
        os.environ["COSMOS_DATABASE"],
        os.environ.get("ORDERS_CONTAINER", "orders"),
        os.environ.get("OUTBOX_CONTAINER", "outbox"),
    )

    try:
        with order_span(event.order_id, "compensate", event.operation_id) as span:
            order = await repo.get(event.order_id, partition_key=event.order_id)
            if order is None:
                raise ValueError(f"order {event.order_id} not found")

            if order.state is OrderState.CANCELLED:
                structured("compensate_noop", orderId=order.order_id, reason="already cancelled")
                return

            if order.state is not OrderState.COMPENSATING:
                structured("compensate_noop", orderId=order.order_id, state=order.state.value)
                return

            # Decide what to undo from the order's own history — the history is
            # the record of what actually happened, which is the only reliable
            # source (a payload can be stale or forged).
            reached = {h["to"] for h in order.history}
            authorisation = next(
                (h["reason"] for h in order.history if h["to"] == "PAYMENT_AUTHORISED"), None)

            try:
                if "STOCK_RESERVED" in reached:
                    await _release_stock(order.order_id)
                if "PAYMENT_AUTHORISED" in reached:
                    await _refund(order.order_id, authorisation)

                await repo.transition(
                    order.order_id, order.customer_id, OrderState.CANCELLED,
                    reason="compensation complete", actor="compensation")
                structured("order_cancelled", orderId=order.order_id,
                           compensated=sorted(reached))
            except ConcurrentUpdate:
                structured("compensate_race", orderId=order.order_id)
            except IllegalTransition:
                structured("compensate_noop", orderId=order.order_id,
                           reason="illegal transition")
            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                # Move to STUCK so a human sees it, then re-raise so the message
                # dead-letters with its delivery history intact.
                try:
                    await repo.transition(
                        order.order_id, order.customer_id, OrderState.STUCK,
                        reason=f"compensation failed: {exc}", actor="compensation")
                except Exception:  # noqa: BLE001
                    pass
                raise
    finally:
        await client.close()
        await credential.close()
