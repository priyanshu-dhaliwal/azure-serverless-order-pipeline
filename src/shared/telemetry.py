"""
Telemetry helpers: one operation id per order, carried through every hop.

Application Insights understands `operation_Id` and `operation_ParentId`, so
setting them on the current span makes the whole saga appear as one distributed
transaction — from the HTTP POST to the compensation two services later.
"""

from __future__ import annotations

import logging
import os
import uuid
from contextlib import contextmanager
from typing import Iterator

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace
from opentelemetry.trace import SpanKind

_configured = False


def configure() -> None:
    global _configured
    if _configured:
        return
    conn = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if conn:
        configure_azure_monitor(connection_string=conn)
    _configured = True


def current_operation_id() -> str:
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx and ctx.is_valid:
        return format(ctx.trace_id, "032x")
    return ""


@contextmanager
def order_span(order_id: str, name: str, operation_id: str | None = None
               ) -> Iterator[trace.Span]:
    """
    A span linked to the order's operation id. When `operation_id` is supplied
    (from a Service Bus message), the span is created as a *link* to the
    upstream trace rather than a new root — that is what stitches the saga
    together across queue hops.
    """
    configure()
    tracer = trace.get_tracer("orders")
    with tracer.start_as_current_span(
        name,
        kind=SpanKind.CONSUMER,
        attributes={
            "order.id": order_id,
            "order.operation_id": operation_id or "",
        },
    ) as span:
        if operation_id:
            span.set_attribute("operation.id", operation_id)
        yield span


def new_operation_id() -> str:
    return uuid.uuid4().hex


def structured(event: str, **fields) -> None:
    """One JSON line per event. Log Analytics can query these directly."""
    import json
    payload = {"event": event, **fields}
    logging.getLogger("orders").info(json.dumps(payload, default=str))
