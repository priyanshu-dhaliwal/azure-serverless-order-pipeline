"""
Saga advance: turn a fully-authorised order into CONFIRMED, or compensate.

This is deliberately a separate function from the step worker. It is the only
place that decides "this order is done", which means there is exactly one place
to look when an order is neither confirmed nor cancelled.

It runs on the same session queue: the ConfirmationRequested event is written to
the outbox by the step worker after PAYMENT_AUTHORISED, and arrives here in
order for that session.
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

log = logging.getLogger("orders.saga")


async def main(msg: func.ServiceBusMessage) -> None:
    body = json.loads(msg.get_body().decode("utf-8"))
    props = msg.application_properties or {}
    event = OrderEvent.from_message_body(body, operation_id=props.get("operationId", ""))

    if event.event_type != "ConfirmRequested":
        # Anything else on this queue is a programming error, not a transient
        # failure: raise so it dead-letters rather than being swallowed.
        raise ValueError(f"saga_advance received unexpected event {event.event_type}")

    credential = DefaultAzureCredential()
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=credential)
    repo = OrderRepository(
        client,
        os.environ["COSMOS_DATABASE"],
        os.environ.get("ORDERS_CONTAINER", "orders"),
        os.environ.get("OUTBOX_CONTAINER", "outbox"),
    )

    try:
        with order_span(event.order_id, "saga_confirm", event.operation_id):
            order = await repo.get(event.order_id, partition_key=event.order_id)
            if order is None:
                raise ValueError(f"order {event.order_id} not found")

            if order.state is OrderState.CONFIRMED:
                structured("saga_noop", orderId=order.order_id, reason="already confirmed")
                return

            if order.state is not OrderState.PAYMENT_AUTHORISED:
                # Not ready to confirm. Compensation is decided by the step
                # worker, not here; this event is simply early or stale.
                structured("saga_noop", orderId=order.order_id, state=order.state.value)
                return

            try:
                await repo.transition(
                    order.order_id, order.customer_id, OrderState.CONFIRMED,
                    reason="stock reserved and payment authorised", actor="saga")
                structured("order_confirmed", orderId=order.order_id,
                           total=str(order.total))
            except ConcurrentUpdate:
                structured("saga_race", orderId=order.order_id)
            except IllegalTransition:
                structured("saga_noop", orderId=order.order_id, reason="illegal transition")
    finally:
        await client.close()
        await credential.close()
