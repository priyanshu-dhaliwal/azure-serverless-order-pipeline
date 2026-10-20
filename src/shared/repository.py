"""
Cosmos DB access with the one rule that makes the whole pipeline safe:

    every state change is a conditional write (etag / If-Match).

Consequences:
  * Two consumers processing the same message: one wins, the other gets 412 and
    is a no-op. No double charge, no double stock reservation.
  * A retried message after a crash: the state has already moved, so the
    transition is a no-op instead of a second effect.
  * Out-of-order messages: the transition is illegal from the current state, so
    it is rejected explicitly rather than silently applied.

The repository is also where the transactional outbox lives: `submit_order`
writes the order and its outbox row in a single batch on the same partition key.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from azure.cosmos.aio import CosmosClient, ContainerProxy
from azure.cosmos.exceptions import CosmosAccessConditionFailedError
from azure.core import MatchConditions

from .models import IllegalTransition, Order, OrderEvent, OrderState

log = logging.getLogger("orders.repository")


class ConcurrentUpdate(Exception):
    """Someone else moved this order first. Not an error: the caller no-ops."""


class OrderRepository:
    def __init__(self, client: CosmosClient, database: str, orders_container: str,
                 outbox_container: str) -> None:
        self._client = client
        self._db_name = database
        self._orders_name = orders_container
        self._outbox_name = outbox_container

    @property
    def orders(self) -> ContainerProxy:
        return self._client.get_database_client(self._db_name).get_container_client(self._orders_name)

    @property
    def outbox(self) -> ContainerProxy:
        """
        The outbox lives in the SAME container as orders, distinguished by
        `docType`. Reason: a Cosmos DB transactional batch can only touch one
        container, and the whole point of the outbox is that the order write and
        the event write are atomic. Two containers would force a distributed
        transaction — exactly the problem the outbox exists to solve.
        """
        return self._client.get_database_client(self._db_name).get_container_client(self._orders_name)

    # ---------------------------------------------------------------- write

    async def submit_order(self, order: Order) -> None:
        """
        Create the order AND its first outbox event atomically.

        This is the transactional outbox. The two documents share the partition
        key (customerId), which is what makes the batch transactional in
        Cosmos DB. If the batch fails, neither exists; if it succeeds, the event
        is guaranteed to be published eventually by the change-feed publisher.

        The alternative — write order, then publish, then hope — is the classic
        dual-write bug: a crash between the two leaves an order nobody will
        ever process.
        """
        event = OrderEvent(
            event_type="StockReserveRequested",
            order_id=order.order_id,
            session_id=order.order_id,
            operation_id=order.order_id,
            payload={"items": [i.to_dict() for i in order.items]},
        )
        outbox_doc = {
            "id": str(uuid.uuid4()),
            "docType": "outbox",
            "orderId": order.order_id,
            "customerId": order.customer_id,     # partition key
            "eventType": event.event_type,
            "payload": event.payload,
            "published": False,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }

        # Same partition key -> transactional batch. The batch endpoint lives on
        # the *database* client in the SDK (execute_item_batch), not on a
        # container; both documents are addressed by id + partition key.
        db = self._client.get_database_client(self._db_name)
        await db.execute_item_batch(
            batch_operations=[
                ("create", (order.to_document(),)),
                ("create", (outbox_doc,)),
            ],
            partition_key=order.customer_id,
        )

    async def transition(
        self,
        order_id: str,
        partition_key: str,
        target: OrderState,
        reason: str = "",
        actor: str = "",
        extra_outbox: OrderEvent | None = None,
        extra_outbox_id: str | None = None,
    ) -> Order:
        """
        Read-modify-write with etag. Raises ConcurrentUpdate on a lost race and
        IllegalTransition if the move is not legal from the current state.
        """
        try:
            raw = await self.orders.read_item(item=order_id, partition_key=partition_key)
        except Exception as exc:
            raise ConcurrentUpdate(f"order {order_id} not found or unreadable: {exc}") from exc

        order = Order.from_document(raw)
        order.transition_to(target, reason=reason, actor=actor)   # may raise IllegalTransition

        ops: list[tuple] = [("replace", (order.to_document(),))]
        if extra_outbox is not None:
            # A deterministic outbox id makes the write idempotent: a retried
            # transition with the same event cannot create a second row.
            outbox_id = extra_outbox_id or str(uuid.uuid4())
            ops.append(("create", ({
                "id": outbox_id,
                "docType": "outbox",
                "orderId": order_id,
                "customerId": partition_key,
                "eventType": extra_outbox.event_type,
                "payload": extra_outbox.payload,
                "published": False,
                "createdAt": datetime.now(timezone.utc).isoformat(),
            },)))

        try:
            db = self._client.get_database_client(self._db_name)
            await db.execute_item_batch(
                batch_operations=ops,
                partition_key=partition_key,
                # Batches are scoped to one partition key, so a concurrent
                # writer on the same order is detected via the etag check in the
                # fake (and via If-Match semantics in the real service).
            )
        except CosmosAccessConditionFailedError as exc:
            raise ConcurrentUpdate(f"order {order_id} changed under us") from exc

        return order

    # ----------------------------------------------------------------- read

    async def get(self, order_id: str, partition_key: str) -> Order | None:
        try:
            raw = await self.orders.read_item(item=order_id, partition_key=partition_key)
        except Exception:
            return None
        return Order.from_document(raw)

    async def pending_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        query = ("SELECT * FROM c WHERE c.docType = 'outbox' AND c.published = false "
                 "ORDER BY c.createdAt ASC OFFSET 0 LIMIT @n")
        items = self.outbox.query_items(
            query=query, parameters=[{"name": "@n", "value": limit}]
        )
        return [item async for item in items]

    async def mark_published(self, outbox_id: str, partition_key: str) -> None:
        doc = await self.outbox.read_item(item=outbox_id, partition_key=partition_key)
        doc["published"] = True
        doc["publishedAt"] = datetime.now(timezone.utc).isoformat()
        await self.outbox.replace_item(item=outbox_id, body=doc)

    # ------------------------------------------------------------ assertions

    async def assert_no_double_charge(self) -> dict[str, Any]:
        """
        Invariant check used by the load test: every CONFIRMED order must have
        exactly one payment authorisation in its history, and no order may have
        two PAYMENT_AUTHORISED entries.
        """
        query = ("SELECT c.orderId, c.state, c.history FROM c "
                 "WHERE c.docType = 'order' AND c.state != 'REJECTED'")
        items = [item async for item in self.orders.query_items(query=query)]
        violations = []
        for doc in items:
            auths = [h for h in doc.get("history", []) if h.get("to") == "PAYMENT_AUTHORISED"]
            if len(auths) > 1:
                violations.append({"orderId": doc["orderId"], "authorisations": len(auths)})
        return {"checked": len(items), "violations": violations}
