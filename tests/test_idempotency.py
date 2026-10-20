"""
Tests for the properties that make at-least-once delivery safe.

These run without Azure. The Cosmos container is a fake that enforces the one
behaviour the correctness argument depends on: conditional replace with etag.

What is asserted here is what would page someone at 3 a.m. if it broke:
  * a duplicate message never double-charges
  * an out-of-order message never corrupts state
  * two workers racing produce exactly one effect
  * the order + outbox write is atomic
"""

from __future__ import annotations

import asyncio
import copy
import sys
import types
import importlib.util

import pytest


# ---- stub azure.cosmos if it is not installed -----------------------------
def _stub(name: str, **attrs) -> None:
    if name in sys.modules:
        return
    try:
        if importlib.util.find_spec(name) is not None:
            return
    except ModuleNotFoundError:
        pass
    mod = types.ModuleType(name)
    mod.__spec__ = importlib.util.spec_from_loader(name, loader=None)
    mod.__path__ = []
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod


class _AccessConditionFailed(Exception):
    pass


_stub("azure", )
_stub("azure.cosmos")
_stub("azure.cosmos.aio", CosmosClient=object, ContainerProxy=object)
_stub("azure.cosmos.exceptions", CosmosAccessConditionFailedError=_AccessConditionFailed)
_stub("azure.core", MatchConditions=object)

sys.path.insert(0, "src")

from shared.models import (  # noqa: E402
    IllegalTransition, Order, OrderItem, OrderState, TRANSITIONS, TERMINAL,
)
from shared.repository import ConcurrentUpdate, OrderRepository  # noqa: E402
from decimal import Decimal  # noqa: E402


# ---------------------------------------------------------------------------
# Fake Cosmos with etag semantics
# ---------------------------------------------------------------------------

class FakeContainer:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}
        self._etag = 0
        self.batches = 0

    def _bump(self) -> str:
        self._etag += 1
        return str(self._etag)

    async def read_item(self, item, partition_key):
        if item not in self.items:
            raise KeyError(item)
        return copy.deepcopy(self.items[item])

    async def upsert_item(self, body):
        body = copy.deepcopy(body)
        body["_etag"] = self._bump()
        self.items[body["id"]] = body
        return body


class FakeCosmos:
    def __init__(self) -> None:
        self.containers = {"orders": FakeContainer(), "outbox": FakeContainer()}

    async def execute_item_batch(self, batch_operations, partition_key, **kwargs):
        """Apply all ops or none — and honour the etag precondition."""
        self.containers["orders"].batches += 1
        staged = copy.deepcopy(self.containers["orders"].items)
        # A Cosmos transactional batch operates on ONE container. Both the order
        # and its outbox row are the same document type in `orders` (the outbox
        # is a document, not a second container), so the batch is single-container.
        #
        # Every doc in a batch shares the partition key — model that explicitly
        # so a cross-partition bug in the repository fails here.
        for _, (doc,) in batch_operations:
            if doc.get("customerId") != partition_key:
                raise ValueError("cross-partition batch")

        container = self.containers["orders"]
        staged = copy.deepcopy(container.items)
        for op, (doc,) in batch_operations:
            if op == "create":
                if doc["id"] in staged:
                    raise _AccessConditionFailed(f"duplicate id {doc['id']}")
                staged[doc["id"]] = copy.deepcopy(doc)
            elif op == "replace":
                existing = staged.get(doc["id"])
                if existing is None:
                    raise _AccessConditionFailed("missing")
                if doc.get("_etag") and existing.get("_etag") != doc["_etag"]:
                    raise _AccessConditionFailed("etag mismatch")
                staged[doc["id"]] = copy.deepcopy(doc)
        for doc_id, doc in staged.items():
            doc["_etag"] = container._bump()
        container.items = staged
        container.batches += 1
        return []


    def get_database_client(self, _):
        return self

    from collections import Counter as _Counter
    calls = _Counter()

    def get_container_client(self, name):
        self.calls[name] += 1
        return self.containers[name.split("-")[0]]   # "outbox-leases" -> outbox


@pytest.fixture
def repo():
    return OrderRepository(FakeCosmos(), "db", "orders", "outbox")


def make_order(order_id="o-1", customer="c-1") -> Order:
    return Order(
        order_id=order_id,
        customer_id=customer,
        items=[OrderItem(sku="sku-1", qty=2, price=Decimal("499.00"))],
        payment_token="tok_ok",
    )


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

def test_happy_path_transitions_are_legal():
    o = make_order()
    for target in (OrderState.STOCK_RESERVED, OrderState.PAYMENT_AUTHORISED,
                   OrderState.CONFIRMED):
        o.transition_to(target)
    assert o.state is OrderState.CONFIRMED
    assert o.is_terminal


def test_confirmed_is_terminal_and_cannot_be_replayed_into_compensation():
    """A duplicate ConfirmRequested arriving after CANCELLED must not resurrect
    the order, and a late payment must not un-cancel it."""
    o = make_order()
    o.transition_to(OrderState.STOCK_RESERVED)
    o.transition_to(OrderState.COMPENSATING)
    o.transition_to(OrderState.CANCELLED)
    with pytest.raises(IllegalTransition):
        o.transition_to(OrderState.PAYMENT_AUTHORISED)


def test_cannot_skip_stock_reservation():
    """Payment before stock is reserved is exactly the out-of-order case the
    session queue is supposed to prevent — but the state machine must also
    refuse it, because sessions protect ordering, not redelivery."""
    o = make_order()
    with pytest.raises(IllegalTransition):
        o.transition_to(OrderState.PAYMENT_AUTHORISED)


def test_every_state_has_a_defined_exit_or_is_terminal():
    reachable = {OrderState.CREATED}
    changed = True
    while changed:
        changed = False
        for src in list(reachable):
            for dst in TRANSITIONS.get(src, set()):
                if dst not in reachable:
                    reachable.add(dst)
                    changed = True
    assert reachable == set(OrderState), f"unreachable states: {set(OrderState) - reachable}"
    for state in OrderState:
        assert TRANSITIONS.get(state) is not None
        if state not in TERMINAL:
            assert TRANSITIONS[state], f"{state} has no exit — saga could hang"


def test_money_is_decimal_not_float():
    o = make_order()
    assert o.total == Decimal("998.00")
    assert str(o.total) == "998.00"
    # 0.1 + 0.2 problem, which is why prices are never floats
    a = Order(order_id="a", customer_id="c", payment_token="t",
              items=[OrderItem("s", 1, Decimal("0.1")), OrderItem("s", 1, Decimal("0.2"))])
    assert a.total == Decimal("0.3")


# ---------------------------------------------------------------------------
# Idempotency: the core property
# ---------------------------------------------------------------------------

def test_duplicate_transition_is_a_noop_not_a_second_effect(repo):
    """The single most important test in this repository."""
    o = make_order()
    asyncio.run(repo.submit_order(o))

    asyncio.run(repo.transition("o-1", "c-1", OrderState.STOCK_RESERVED))
    with pytest.raises(IllegalTransition):
        asyncio.run(repo.transition("o-1", "c-1", OrderState.STOCK_RESERVED))

    doc = repo.orders.items["o-1"]
    reserved = [h for h in doc["history"] if h["to"] == "STOCK_RESERVED"]
    assert len(reserved) == 1, "duplicate message produced a second reservation"


def test_two_workers_racing_produce_one_effect(repo):
    o = make_order()
    asyncio.run(repo.submit_order(o))

    async def race():
        results = await asyncio.gather(
            repo.transition("o-1", "c-1", OrderState.STOCK_RESERVED),
            repo.transition("o-1", "c-1", OrderState.STOCK_RESERVED),
            return_exceptions=True,
        )
        return results

    results = asyncio.run(race())
    winners = [r for r in results if not isinstance(r, Exception)]
    losers = [r for r in results if isinstance(r, Exception)]
    assert len(winners) == 1, f"expected exactly one winner, got {len(winners)}"
    assert all(isinstance(e, (ConcurrentUpdate, IllegalTransition)) for e in losers)
    assert len([h for h in repo.orders.items["o-1"]["history"]
                if h["to"] == "STOCK_RESERVED"]) == 1


def test_payment_idempotency_key_is_stable_across_retries():
    """The key is generated once and persisted. A retried payment sends the same
    key, so the provider returns the original authorisation instead of charging
    again."""
    o = make_order()
    key = o.payment_idempotency_key
    round_tripped = Order.from_document(o.to_document())
    assert round_tripped.payment_idempotency_key == key
    assert key  # not empty, not regenerated on deserialisation


# ---------------------------------------------------------------------------
# Transactional outbox
# ---------------------------------------------------------------------------

def _outbox_rows(repo, order_id="o-1"):
    return [d for d in repo.outbox.items.values()
            if d.get("docType") == "outbox" and d["orderId"] == order_id]


def test_order_and_outbox_are_written_atomically(repo):
    o = make_order()
    asyncio.run(repo.submit_order(o))
    assert "o-1" in repo.orders.items
    rows = _outbox_rows(repo)
    assert len(rows) == 1
    assert rows[0]["eventType"] == "StockReserveRequested"
    assert rows[0]["published"] is False


def test_outbox_shares_the_orders_container_so_the_batch_is_atomic(repo):
    """A batch cannot span containers. If the outbox were its own container the
    order+event write would need a distributed transaction — the exact problem
    the outbox pattern exists to avoid."""
    assert repo.outbox is repo.orders


def test_outbox_row_is_not_created_if_the_order_already_exists(repo):
    """A retried HTTP POST with the same order id must not create a second order
    or a second event — the batch fails as a unit."""
    o = make_order()
    asyncio.run(repo.submit_order(o))
    with pytest.raises(Exception):
        asyncio.run(repo.submit_order(o))
    assert len(_outbox_rows(repo)) == 1


def test_transition_and_next_event_are_atomic(repo):
    """If the transition fails, the follow-on event must not be queued — else
    the next step runs against a state that never happened."""
    from shared.models import OrderEvent

    o = make_order()
    asyncio.run(repo.submit_order(o))

    bogus = OrderEvent(event_type="PaymentRequested", order_id="o-1", session_id="o-1",
                       operation_id="op-1", payload={})
    with pytest.raises(IllegalTransition):
        asyncio.run(repo.transition("o-1", "c-1", OrderState.CONFIRMED,
                                    extra_outbox=bogus, extra_outbox_id="pay-o-1"))
    assert "pay-o-1" not in repo.outbox.items
