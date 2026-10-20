"""
Step worker: handles StockReserveRequested and PaymentRequested.

Both events are "do one thing, then advance the saga". The handler is written as
three guarded steps, and each guard is what makes at-least-once delivery safe:

  1. Load the order. If it is already past this step → no-op, complete the
     message. (Duplicate delivery after a successful run.)
  2. Do the external effect with an idempotency key derived from the order.
     (Retry of an effect that may already have happened.)
  3. Conditional transition: order state -> next state, plus the outbox event
     for the next step, in one transactional batch. If another consumer won,
     we get ConcurrentUpdate and treat it as a no-op.

The queue is session-enabled with session_id = order_id, so for a given order
these run one at a time. The guards above are still required, because sessions
protect against *ordering*, not against *redelivery*.

Poison handling: messages that raise reach maxDeliveryCount and land in the
dead-letter queue, where `scripts/replay-dlq.py` can inspect and replay them.
Nothing is dropped silently.
"""

from __future__ import annotations

import json
import logging
import os
import random

import azure.functions as func
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential

from shared.models import OrderEvent, OrderState
from shared.models import IllegalTransition
from shared.repository import ConcurrentUpdate, OrderRepository
from shared.telemetry import order_span, structured

log = logging.getLogger("orders.steps")

# Simulated downstream failure rate, for drills. 0 in production.
PAYMENT_FAILURE_RATE = float(os.environ.get("PAYMENT_FAILURE_RATE", "0"))
STOCK_FAILURE_RATE = float(os.environ.get("STOCK_FAILURE_RATE", "0"))


class ExternalFailure(Exception):
    """The downstream system rejected the operation. Compensation territory."""


async def _reserve_stock(order_id: str, items: list[dict]) -> str:
    """
    Stand-in for the inventory service call.

    Idempotency: the inventory service keys on order_id, so a retried reserve
    returns the original reservation instead of reserving twice. That contract
    is why we can retry this safely.
    """
    if random.random() < STOCK_FAILURE_RATE:
        raise ExternalFailure("inventory: insufficient stock")
    return f"res-{order_id[:8]}"


async def _authorise_payment(order_id: str, idempotency_key: str, amount: str,
                             token: str) -> str:
    """
    Stand-in for the payment provider.

    The idempotency key is generated once when the order is created and stored
    on the order document, so every retry sends the SAME key. A provider that
    honours it will return the original authorisation rather than charging
    again — which is the difference between at-least-once delivery and a
    double-charge incident.
    """
    if random.random() < PAYMENT_FAILURE_RATE:
        raise ExternalFailure("payment: card declined")
    return f"auth-{idempotency_key[-12:]}"


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
        with order_span(event.order_id, f"step:{event.event_type}", event.operation_id) as span:
            span.set_attribute("message.delivery_count", msg.delivery_count or 0)

            order = await repo.get(event.order_id, partition_key=event.order_id)
            if order is None:
                # Unknown order: never silently succeed. Let it dead-letter.
                raise ValueError(f"order {event.order_id} not found")

            if event.event_type == "StockReserveRequested":
                # Guard 1: already done?
                if order.state != OrderState.CREATED:
                    structured("step_noop", orderId=order.order_id, eventType=event.event_type,
                               state=order.state.value)
                    return

                try:
                    reservation = await _reserve_stock(order.order_id, body["payload"]["items"])
                except ExternalFailure as exc:
                    await _compensate(repo, order.order_id, order.customer_id, str(exc),
                                      already_charged=False)
                    structured("step_failed", orderId=order.order_id, step="stock", error=str(exc))
                    return

                next_event = OrderEvent(
                    event_type="PaymentRequested",
                    order_id=order.order_id,
                    session_id=order.order_id,
                    operation_id=event.operation_id,
                    payload={"amount": str(order.total)},
                )
                try:
                    await repo.transition(
                        order.order_id, order.customer_id, OrderState.STOCK_RESERVED,
                        reason=f"reserved {reservation}", actor="inventory",
                        extra_outbox=next_event,
                        extra_outbox_id=f"pay-{order.order_id}")
                except ConcurrentUpdate:
                    structured("step_race", orderId=order.order_id, step="stock")
                except IllegalTransition:
                    # Out-of-order message: the order already moved on.
                    structured("step_noop", orderId=order.order_id, step="stock",
                               reason="illegal transition from current state")
                    return

            elif event.event_type == "PaymentRequested":
                if order.state != OrderState.STOCK_RESERVED:
                    structured("step_noop", orderId=order.order_id, eventType=event.event_type,
                               state=order.state.value)
                    return

                try:
                    auth = await _authorise_payment(
                        order.order_id, order.payment_idempotency_key,
                        str(order.total), order.payment_token)
                except ExternalFailure as exc:
                    # Payment failed -> compensate the reservation we already made.
                    await _compensate(repo, order.order_id, order.customer_id, str(exc),
                                      already_charged=False)
                    structured("step_failed", orderId=order.order_id, step="payment",
                               error=str(exc))
                    return

                confirm = OrderEvent(
                    event_type="ConfirmRequested",
                    order_id=order.order_id,
                    session_id=order.order_id,
                    operation_id=event.operation_id,
                    payload={"authorisation": auth},
                )
                try:
                    await repo.transition(
                        order.order_id, order.customer_id, OrderState.PAYMENT_AUTHORISED,
                        reason=f"authorised {auth}", actor="payment",
                        extra_outbox=confirm,
                        extra_outbox_id=f"confirm-{order.order_id}")
                except ConcurrentUpdate:
                    structured("step_race", orderId=order.order_id, step="payment")

            else:
                # Unknown event type on this queue: dead-letter rather than guess.
                raise ValueError(f"unexpected event type {event.event_type}")

    finally:
        await client.close()
        await credential.close()


async def _compensate(repo: OrderRepository, order_id: str, customer_id: str,
                      reason: str, already_charged: bool) -> None:
    """
    Enter COMPENSATING and queue the compensation events.

    Both compensation events go through the outbox, so they survive a crash
    between "we decided to compensate" and "the compensation ran". Deciding to
    compensate and then losing the decision is how customers get charged for
    orders that failed.
    """
    compensation = OrderEvent(
        event_type="Compensate",
        order_id=order_id,
        session_id=order_id,
        operation_id=order_id,
        payload={"reason": reason, "alreadyCharged": already_charged},
    )

    try:
        # Transition AND write the compensation outbox row in one transactional
        # batch. If the process dies here, either both happened or neither did —
        # we never end up in COMPENSATING with no compensation queued.
        await repo.transition(
            order_id, customer_id, OrderState.COMPENSATING,
            reason=reason, actor="saga", extra_outbox=compensation,
            extra_outbox_id=f"comp-{order_id}")
    except ConcurrentUpdate:
        # Someone else already moved it; their compensation stands.
        return
