"""
Change-feed publisher: outbox -> Service Bus.

This is the second half of the transactional outbox. It runs on the Cosmos DB
change feed, which is push-based and ordered within a partition, so:

  * an outbox row cannot be missed (it is in the feed forever within retention),
  * a crash mid-publish simply re-delivers the row on restart,
  * a duplicate publish is harmless because consumers are idempotent.

We mark the row published only AFTER the send succeeds. If we crash between, the
row is re-sent — which is correct, and only correct because of idempotency.
"""

from __future__ import annotations

import json
import logging
import os

import azure.functions as func
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential

from shared.messaging import Publisher
from shared.repository import OrderRepository
from shared.telemetry import order_span, structured

log = logging.getLogger("orders.outbox")


async def main(documents: func.DocumentList) -> None:
    if not documents:
        return

    credential = DefaultAzureCredential()
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=credential)
    repo = OrderRepository(
        client,
        os.environ["COSMOS_DATABASE"],
        os.environ.get("ORDERS_CONTAINER", "orders"),
        os.environ.get("OUTBOX_CONTAINER", "outbox"),
    )
    publisher = Publisher()

    published = 0
    failed = 0
    try:
        for doc in documents:
            if doc.get("docType") != "outbox" or doc.get("published"):
                continue        # not an event, or another instance handled it

            event_type = doc["eventType"]
            queue = (
                os.environ["COMPENSATION_QUEUE"]
                if event_type in ("StockReleaseRequested", "RefundRequested")
                else os.environ["SESSION_QUEUE"]
            )
            message = {
                "body": {
                    "eventType": event_type,
                    "orderId": doc["orderId"],
                    "payload": doc.get("payload", {}),
                    "operationId": doc["orderId"],
                },
                "session_id": doc["orderId"],
                "application_properties": {
                    "eventType": event_type,
                    "orderId": doc["orderId"],
                    "operationId": doc["orderId"],
                },
            }

            try:
                with order_span(doc["orderId"], f"publish:{event_type}", doc["orderId"]):
                    await publisher.publish(message, queue=queue)
                await repo.mark_published(doc["id"], doc["customerId"])
                published += 1
                structured("outbox_published", orderId=doc["orderId"], eventType=event_type,
                           queue=queue)
            except Exception as exc:  # noqa: BLE001
                # Leave unpublished: the change feed will deliver it again.
                failed += 1
                log.error("publish failed for outbox %s (%s): %s", doc["id"], event_type, exc)
                structured("outbox_publish_failed", orderId=doc["orderId"],
                           eventType=event_type, error=str(exc))

        structured("outbox_batch", published=published, failed=failed,
                   total=len(documents))
    finally:
        await publisher.close()
        await client.close()
        await credential.close()
