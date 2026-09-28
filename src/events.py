"""Publishes node lifecycle events to RabbitMQ.

The API talks to the broker through the default exchange, routing straight to the
"node_events" queue. One long-lived connection is shared by every request thread,
guarded by a lock because pika's BlockingConnection is not thread-safe.
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone

import pika
from pika.exceptions import AMQPError

QUEUE_NAME = "node_events"
NODE_REGISTERED = "node_registered"
NODE_DELETED = "node_deleted"

logger = logging.getLogger("uvicorn.error")


def utc_timestamp() -> str:
    """ISO 8601 in UTC with a trailing Z, e.g. 2026-05-01T12:00:00Z."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class EventPublisher:
    def __init__(self, url: str, attempts: int = 2):
        self._params = pika.URLParameters(url)
        self._attempts = attempts
        self._lock = threading.Lock()
        self._connection = None
        self._channel = None

    def _open_channel(self):
        if self._channel is not None and self._channel.is_open:
            return self._channel
        self._close()
        self._connection = pika.BlockingConnection(self._params)
        channel = self._connection.channel()
        # Same declaration as the consumer: whoever starts first creates the queue,
        # so no event is lost to an unroutable publish.
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        # Publisher confirms: basic_publish only returns once the broker has the message.
        channel.confirm_delivery()
        self._channel = channel
        return channel

    def _close(self):
        if self._connection is not None and self._connection.is_open:
            try:
                self._connection.close()
            except (AMQPError, OSError):
                pass
        self._connection = None
        self._channel = None

    def publish(self, event: str, node_name: str) -> bool:
        """Send one event. Returns False (and logs) if the broker could not take it.

        The node is already committed when this runs, so a broker outage must not
        turn a successful registration into an HTTP error.
        """
        body = json.dumps({"event": event, "node_name": node_name, "timestamp": utc_timestamp()})
        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
        )
        with self._lock:
            # A second attempt covers the common failure: the broker dropped an idle
            # connection (missed heartbeats) and the cached channel is stale.
            for attempt in range(1, self._attempts + 1):
                try:
                    self._open_channel().basic_publish(
                        exchange="", routing_key=QUEUE_NAME, body=body, properties=properties
                    )
                    return True
                except (AMQPError, OSError) as exc:
                    # OSError: with the broker container gone, pika lets DNS failures
                    # (socket.gaierror) through unwrapped.
                    logger.warning("Publish of %s for %s failed (attempt %d/%d): %r",
                                   event, node_name, attempt, self._attempts, exc)
                    self._close()
        logger.error("Event %s for %s was not delivered to RabbitMQ", event, node_name)
        return False

    def close(self):
        with self._lock:
            self._close()


# Credentials come only from the environment (see .env.example), never from code.
publisher = EventPublisher(os.environ["RABBITMQ_URL"])
