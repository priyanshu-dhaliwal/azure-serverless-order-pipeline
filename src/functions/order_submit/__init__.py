"""
HTTP trigger: POST /api/orders

Does exactly two things, atomically:
  1. validates the request and creates the order document in CREATED
  2. writes the first outbox event (StockReserveRequested)

and then returns 202 with the order id. It does NOT call inventory or payment:
the request path must not wait on downstream services, and a failure downstream
must not fail the user's request — it must be compensated.

POST /api/orders/{id}?customerId=... is the read model (see order_status).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from decimal import Decimal, InvalidOperation

import azure.functions as func
from azure.cosmos.aio import CosmosClient
from azure.identity.aio import DefaultAzureCredential
from pydantic import BaseModel, Field, field_validator

from shared.models import Order, OrderItem
from shared.repository import OrderRepository
from shared.telemetry import order_span, structured, new_operation_id

log = logging.getLogger("orders.submit")

MAX_ITEMS = 50
MAX_QTY = 1000


class ItemModel(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    qty: int = Field(gt=0, le=MAX_QTY)
    price: str | float | int

    @field_validator("price")
    @classmethod
    def price_is_money(cls, v):
        try:
            d = Decimal(str(v))
        except InvalidOperation as exc:
            raise ValueError("price must be a number") from exc
        if d < 0:
            raise ValueError("price must not be negative")
        return str(d)


class OrderModel(BaseModel):
    customerId: str = Field(min_length=1, max_length=64)
    items: list[ItemModel] = Field(min_length=1)
    paymentToken: str = Field(min_length=1)

    @field_validator("items")
    @classmethod
    def not_too_many(cls, v):
        if len(v) > MAX_ITEMS:
            raise ValueError(f"at most {MAX_ITEMS} line items")
        return v


def _bad_request(msg: str, details: list | None = None) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"error": msg, "details": details or []}),
        status_code=400, mimetype="application/json")


async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return _bad_request("body must be JSON")

    try:
        model = OrderModel(**payload)
    except Exception as exc:  # pydantic ValidationError
        details = []
        for err in getattr(exc, "errors", lambda: [])():
            details.append({"field": ".".join(str(p) for p in err["loc"]), "msg": err["msg"]})
        return _bad_request("validation failed", details)

    order = Order(
        order_id=str(uuid.uuid4()),
        customer_id=model.customerId,
        items=[OrderItem(sku=i.sku, qty=i.qty, price=Decimal(i.price)) for i in model.items],
        payment_token=model.paymentToken,
    )

    credential = DefaultAzureCredential()
    client = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=credential)
    repo = OrderRepository(
        client,
        os.environ["COSMOS_DATABASE"],
        os.environ.get("ORDERS_CONTAINER", "orders"),
        os.environ.get("OUTBOX_CONTAINER", "outbox"),
    )

    try:
        with order_span(order.order_id, "order_submit", new_operation_id()) as span:
            await repo.submit_order(order)
            span.set_attribute("order.total", str(order.total))
            span.set_attribute("order.items", len(order.items))
            structured("order_created", orderId=order.order_id, customerId=order.customer_id,
                       total=str(order.total), items=len(order.items))

        return func.HttpResponse(
            json.dumps({
                "orderId": order.order_id,
                "state": order.state.value,
                "total": str(order.total),
                "statusUrl": f"/api/orders/{order.order_id}?customerId={order.customer_id}",
            }),
            status_code=202,
            mimetype="application/json",
            headers={"Location": f"/api/orders/{order.order_id}?customerId={order.customer_id}"},
        )
    except Exception as exc:
        log.exception("submit failed")
        # A failure here means the order was NOT created (the batch is atomic),
        # so 500 with no partial state is correct and safe to retry by the client.
        return func.HttpResponse(
            json.dumps({"error": "could not create order", "detail": type(exc).__name__}),
            status_code=500, mimetype="application/json")
    finally:
        await client.close()
        await credential.close()
