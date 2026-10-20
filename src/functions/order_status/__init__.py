"""
HTTP trigger: GET /api/orders/{id}?customerId=...

The read model. Deliberately a direct point read rather than a projection: the
order document is small, the read is cheap, and a stale projection would be a
second source of truth to reconcile. At high read volume this becomes a
materialised view — noted in the README as a scaling change, not built here.
"""

from __future__ import annotations

import json
import os

import azure.functions as func
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential

from shared.repository import OrderRepository
from shared.telemetry import order_span


async def main(req: func.HttpRequest) -> func.HttpResponse:
    order_id = req.route_params.get("id")
    customer_id = req.params.get("customerId")
    if not order_id or not customer_id:
        return func.HttpResponse(
            json.dumps({"error": "orderId and customerId are required"}),
            status_code=400, mimetype="application/json")

    credential = DefaultAzureCredential()
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=credential)
    repo = OrderRepository(
        client,
        os.environ["COSMOS_DATABASE"],
        os.environ.get("ORDERS_CONTAINER", "orders"),
        os.environ.get("OUTBOX_CONTAINER", "outbox"),
    )
    try:
        with order_span(order_id, "order_status"):
            order = await repo.get(order_id, partition_key=customer_id)
        if order is None:
            return func.HttpResponse(json.dumps({"error": "not found"}),
                                     status_code=404, mimetype="application/json")

        return func.HttpResponse(json.dumps({
            "orderId": order.order_id,
            "state": order.state.value,
            "total": str(order.total),
            "history": order.history,
            "updatedAt": order.updated_at,
        }), mimetype="application/json")
    finally:
        await client.close()
        await credential.close()
