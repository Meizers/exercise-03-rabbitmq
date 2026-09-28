"""
Exercise 03 — Event Consumer

Reads node lifecycle events from the "node_events" queue and logs each one to stdout:
    EVENT: {event} | node: {node_name} | time: {timestamp}

Messages are acknowledged only after they have been logged, so an event is never
lost if the consumer dies mid-message: RabbitMQ redelivers it on the next start.
"""

import json
import logging
import os
import signal
import sys
import time

import pika
from pika.exceptions import AMQPError

QUEUE_NAME = "node_events"
REQUIRED_FIELDS = ("event", "node_name", "timestamp")
INITIAL_BACKOFF = 1
MAX_BACKOFF = 30

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("consumer")
# pika logs every failed connection attempt at ERROR, traceback included, before
# raising; the retry loop below already reports each failure in one line.
logging.getLogger("pika").setLevel(logging.CRITICAL)


class Shutdown(Exception):
    """Raised from the SIGTERM handler to leave the consume loop cleanly."""


def on_sigterm(_signum, _frame):
    raise Shutdown


def parse_event(body: bytes) -> dict:
    event = json.loads(body)
    if not isinstance(event, dict) or any(field not in event for field in REQUIRED_FIELDS):
        raise ValueError(f"expected an object with {', '.join(REQUIRED_FIELDS)}")
    return event


def handle_message(channel, method, _properties, body):
    try:
        event = parse_event(body)
    except ValueError as exc:  # json.JSONDecodeError is a ValueError too
        # Requeueing a malformed message would redeliver it forever; drop it instead.
        log.warning("Discarding malformed message %r: %s", body[:200], exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return
    log.info("EVENT: %s | node: %s | time: %s", event["event"], event["node_name"], event["timestamp"])
    channel.basic_ack(delivery_tag=method.delivery_tag)


def consume(params: pika.URLParameters):
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        # Same declaration as the API, so the queue exists no matter who starts first.
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        # One unacknowledged message at a time: after a crash, at most one event is redelivered.
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
        log.info("Connected to RabbitMQ, waiting for events on '%s'", QUEUE_NAME)
        channel.start_consuming()
    finally:
        # Best effort: the connection may already be half-closed by the error or
        # signal that got us here, and that must not mask the original exception.
        try:
            if connection.is_open:
                connection.close()
        except Exception:
            pass


def main():
    signal.signal(signal.SIGTERM, on_sigterm)
    params = pika.URLParameters(os.environ["RABBITMQ_URL"])
    backoff = INITIAL_BACKOFF
    while True:
        started = time.monotonic()
        try:
            consume(params)
        except (Shutdown, KeyboardInterrupt):
            log.info("Shutting down")
            return
        except (AMQPError, OSError) as exc:
            # OSError: pika lets DNS failures (socket.gaierror) through unwrapped
            # while the broker container is gone.
            if time.monotonic() - started > MAX_BACKOFF:
                # That connection stayed up for a while, so it was healthy: start over.
                backoff = INITIAL_BACKOFF
            log.warning("RabbitMQ connection failed (%r), retrying in %ds", exc, backoff)
        try:
            time.sleep(backoff)
        except Shutdown:
            log.info("Shutting down")
            return
        backoff = min(backoff * 2, MAX_BACKOFF)


if __name__ == "__main__":
    main()
