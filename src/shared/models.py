"""
Domain models and the order state machine.

The state machine is the contract for the whole pipeline: every handler asks it
"is this transition legal from here?" and never writes a state directly. That
means a duplicate message, an out-of-order message, or a retry after a crash all
resolve to the same answer: no-op or compensate, never double-charge.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any


class OrderState(str, Enum):
    CREATED = "CREATED"
    STOCK_RESERVED = "STOCK_RESERVED"
    PAYMENT_AUTHORISED = "PAYMENT_AUTHORISED"
    CONFIRMED = "CONFIRMED"
    # failure paths
    COMPENSATING = "COMPENSATING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    # terminal-ish: needs a human
    STUCK = "STUCK"


# Legal transitions. Anything absent is rejected — this dict is the single place
# where "what may happen next" is defined, and tests assert it is complete.
TRANSITIONS: dict[OrderState, set[OrderState]] = {
    OrderState.CREATED: {OrderState.STOCK_RESERVED, OrderState.REJECTED, OrderState.COMPENSATING},
    OrderState.STOCK_RESERVED: {OrderState.PAYMENT_AUTHORISED, OrderState.COMPENSATING},
    OrderState.PAYMENT_AUTHORISED: {OrderState.CONFIRMED, OrderState.COMPENSATING},
    OrderState.CONFIRMED: set(),                      # terminal
    OrderState.COMPENSATING: {OrderState.CANCELLED, OrderState.STUCK},
    OrderState.CANCELLED: set(),                      # terminal
    OrderState.REJECTED: set(),                       # terminal
    OrderState.STUCK: {OrderState.COMPENSATING},      # human can retry compensation
}

TERMINAL = {s for s, nxt in TRANSITIONS.items() if not nxt}


class IllegalTransition(Exception):
    """Raised when a handler tries to move an order backwards or sideways."""

    def __init__(self, order_id: str, current: OrderState, target: OrderState):
        super().__init__(f"order {order_id}: {current} -> {target} is not allowed")
        self.order_id = order_id
        self.current = current
        self.target = target


@dataclass
class OrderItem:
    sku: str
    qty: int
    price: Decimal

    def __post_init__(self) -> None:
        if self.qty <= 0:
            raise ValueError("qty must be positive")
        # Money as Decimal, serialised as a string. Never a float — and the
        # conversion must happen before any comparison, because a document
        # round-trip brings the price back as a string.
        self.price = Decimal(str(self.price))
        if self.price < 0:
            raise ValueError("price must not be negative")

    def to_dict(self) -> dict[str, Any]:
        return {"sku": self.sku, "qty": self.qty, "price": str(self.price)}


@dataclass
class Order:
    order_id: str
    customer_id: str
    items: list[OrderItem]
    payment_token: str
    state: OrderState = OrderState.CREATED
    # Idempotency key for the *external* payment call. Generated once, persisted,
    # reused on every retry: the payment provider sees the same key and returns
    # the same authorisation instead of charging twice.
    payment_idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    version: int = 0
    history: list[dict[str, Any]] = field(default_factory=list)
    failure_reason: str | None = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def total(self) -> Decimal:
        return sum((i.price * i.qty for i in self.items), Decimal("0"))

    # ------------------------------------------------------------------ state

    def can_transition_to(self, target: OrderState) -> bool:
        return target in TRANSITIONS.get(self.state, set())

    def transition_to(self, target: OrderState, reason: str = "", actor: str = "") -> None:
        if not self.can_transition_to(target):
            raise IllegalTransition(self.order_id, self.state, target)
        now = datetime.now(timezone.utc).isoformat()
        self.history.append({
            "from": self.state.value,
            "to": target.value,
            "at": now,
            "reason": reason,
            "actor": actor,
        })
        self.state = target
        self.updated_at = now
        self.version += 1

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL

    # ------------------------------------------------------------ (de)serialise

    def to_document(self) -> dict[str, Any]:
        return {
            "id": self.order_id,                  # Cosmos document id
            "orderId": self.order_id,
            "customerId": self.customer_id,       # also the partition key
            "items": [i.to_dict() for i in self.items],
            "total": str(self.total),
            "state": self.state.value,
            "paymentToken": self.payment_token,
            "paymentIdempotencyKey": self.payment_idempotency_key,
            "version": self.version,
            "history": self.history,
            "failureReason": self.failure_reason,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        }

    @classmethod
    def from_document(cls, doc: dict[str, Any]) -> "Order":
        order = cls(
            order_id=doc["orderId"],
            customer_id=doc["customerId"],
            items=[OrderItem(**i) for i in doc["items"]],
            payment_token=doc.get("paymentToken", ""),
            state=OrderState(doc["state"]),
            payment_idempotency_key=doc.get("paymentIdempotencyKey", ""),
            version=doc.get("version", 0),
            history=doc.get("history", []),
            failure_reason=doc.get("failureReason"),
            created_at=doc.get("createdAt", ""),
            updated_at=doc.get("updatedAt", ""),
        )
        return order


# ---------------------------------------------------------------------------
# Events carried on Service Bus. `operation_id` rides along so the whole saga
# can be traced in Application Insights as one transaction.
# ---------------------------------------------------------------------------

@dataclass
class OrderEvent:
    event_type: str            # StockReserveRequested | PaymentRequested | ...
    order_id: str
    session_id: str            # == order_id: ordering is per order
    operation_id: str          # trace correlation
    payload: dict[str, Any] = field(default_factory=dict)
    attempt: int = 1

    def to_message(self) -> dict[str, Any]:
        return {
            "body": {
                "eventType": self.event_type,
                "orderId": self.order_id,
                "payload": self.payload,
                "attempt": self.attempt,
            },
            "session_id": self.session_id,
            "application_properties": {
                "eventType": self.event_type,
                "orderId": self.order_id,
                "operationId": self.operation_id,
                "attempt": self.attempt,
            },
        }

    @classmethod
    def from_message_body(cls, body: dict[str, Any], operation_id: str | None = None) -> "OrderEvent":
        return cls(
            event_type=body["eventType"],
            order_id=body["orderId"],
            session_id=body["orderId"],
            operation_id=operation_id or body.get("operationId", ""),
            payload=body.get("payload", {}),
            attempt=body.get("attempt", 1),
        )
