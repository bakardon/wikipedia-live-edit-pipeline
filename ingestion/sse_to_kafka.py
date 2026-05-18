"""Wikimedia EventStreams (SSE) → Kafka topic `wiki.edits`.

- Reconnects with exponential backoff on disconnect.
- Validates each event with pydantic; drops non-edit events and malformed payloads.
- Publishes the *original* Wikimedia JSON envelope as the Kafka value, so the
  downstream Spark job can replay full history if needed.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from typing import Any, Iterator

import requests
from confluent_kafka import Producer
from sseclient import SSEClient

from schema import EditEvent

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("producer")

WIKI_SSE_URL = os.environ.get("WIKI_SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.edits")
# Stop after publishing N edits (0 = unlimited). Useful for capping demo data.
MAX_EVENTS_PUBLISHED = int(os.environ.get("MAX_EVENTS_PUBLISHED", "0"))

_running = True


def _shutdown(signum: int, frame: Any) -> None:
    global _running
    log.info("shutdown signal %s received", signum)
    _running = False


def _delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        log.warning("kafka delivery failed: %s", err)


def _stream_events() -> Iterator[dict[str, Any]]:
    """Yield raw event dicts from SSE, reconnecting on failure."""
    backoff = 1.0
    while _running:
        try:
            log.info("connecting to %s", WIKI_SSE_URL)
            with requests.get(
                WIKI_SSE_URL,
                stream=True,
                headers={
                    "Accept": "text/event-stream",
                    # Wikimedia requires a descriptive User-Agent per their UA policy:
                    # https://foundation.wikimedia.org/wiki/Policy:User-Agent_policy
                    "User-Agent": "wiki-edit-pipeline/0.1 (university DE final project; muhammad808alvi@gmail.com)",
                },
                timeout=(10, 60),
            ) as resp:
                resp.raise_for_status()
                client = SSEClient(resp)
                backoff = 1.0
                for event in client.events():
                    if not _running:
                        return
                    if event.event != "message" or not event.data:
                        continue
                    try:
                        yield json.loads(event.data)
                    except json.JSONDecodeError:
                        continue
        except Exception as exc:  # network, http, etc.
            if not _running:
                return
            log.warning("SSE error: %s; reconnecting in %.1fs", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


def main() -> int:
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKERS,
        "client.id": "wiki-sse-producer",
        "compression.type": "lz4",
        "linger.ms": "50",
        "batch.num.messages": "500",
        "enable.idempotence": "true",
    })

    n_recv = n_pub = n_drop = 0
    last_log = time.time()
    log.info("publishing to %s topic=%s (cap=%s)",
             KAFKA_BROKERS, KAFKA_TOPIC,
             MAX_EVENTS_PUBLISHED if MAX_EVENTS_PUBLISHED > 0 else "unlimited")

    global _running
    for raw_evt in _stream_events():
        n_recv += 1

        try:
            edit = EditEvent.from_event(raw_evt)
        except Exception:
            n_drop += 1
            continue
        if edit is None:
            n_drop += 1
            continue

        try:
            producer.produce(
                KAFKA_TOPIC,
                key=f"{edit.wiki_db}:{edit.page_title}".encode(),
                value=json.dumps(raw_evt).encode(),
                callback=_delivery_report,
            )
            n_pub += 1
        except BufferError:
            producer.poll(0.5)

        producer.poll(0)

        now = time.time()
        if now - last_log > 10:
            log.info("recv=%d pub=%d drop=%d", n_recv, n_pub, n_drop)
            last_log = now

        if MAX_EVENTS_PUBLISHED > 0 and n_pub >= MAX_EVENTS_PUBLISHED:
            log.info("cap reached (n_pub=%d >= %d) — shutting down cleanly",
                     n_pub, MAX_EVENTS_PUBLISHED)
            _running = False
            break

    log.info("flushing producer")
    producer.flush(10)
    log.info("done. recv=%d pub=%d drop=%d", n_recv, n_pub, n_drop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
