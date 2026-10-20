"""Service Bus publishing with retry, session support and trace propagation."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient

log = logging.getLogger("orders.messaging")

NAMESPACE = os.environ.get("SERVICEBUS_FQDN", "")
SESSION_QUEUE = os.environ.get("SESSION_QUEUE", "orders-session")
COMPENSATION_QUEUE = os.environ.get("COMPENSATION_QUEUE", "orders-compensate")


class Publisher:
    """
    A thin wrapper. The important behaviour is *not* the send — it is that a
    failed send raises, so the caller (the change-feed publisher) leaves the
    outbox row unpublished and tries again. Silently swallowing a publish error
    is how orders get lost.
    """

    def __init__(self, namespace_fqdn: str | None = None, credential=None) -> None:
        self._fqdn = namespace_fqdn or NAMESPACE
        self._credential = credential or DefaultAzureCredential()

    async def publish(self, message: dict[str, Any], queue: str | None = None,
                      attempts: int = 5) -> None:
        target = queue or SESSION_QUEUE
        body = message["body"]
        props = message.get("application_properties", {})
        session_id = message.get("session_id")

        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                async with ServiceBusClient(self._fqdn, self._credential) as client:
                    sender = client.get_queue_sender(queue_name=target)
                    async with sender:
                        sb_message = ServiceBusMessage(
                            body=body,
                            session_id=session_id,
                            application_properties=props,
                            # The idempotency key travels with the message so a
                            # redelivery is recognisable at the consumer.
                            message_id=props.get("operationId"),
                        )
                        await sender.send_messages(sb_message)
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                backoff = min(2 ** attempt, 30)
                log.warning("publish to %s failed (attempt %d/%d): %s — retrying in %ds",
                            target, attempt, attempts, exc, backoff)
                await asyncio.sleep(backoff)

        raise RuntimeError(f"publish to {target} failed after {attempts} attempts") from last_exc

    async def close(self) -> None:
        await self._credential.close()
