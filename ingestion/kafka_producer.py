from __future__ import annotations

import json
import logging
import os
import random
import signal
import string
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer

from config.logging import configure_logging
from config.settings import load_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransactionEvent:
    transaction_id: str
    event_time: str
    customer_id: str
    amount: float
    currency: str
    item_count: int
    payment_method: str
    country: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _rid(prefix: str, n: int = 10) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def make_event() -> TransactionEvent:
    payment_method = random.choice(["card", "upi", "cod", "wallet"])
    country = random.choice(["US", "IN", "GB", "DE", "SG", "AU"])
    amount = round(random.lognormvariate(4.3, 0.6), 2)  # realistic skew
    item_count = max(1, int(random.gauss(2.2, 1.0)))
    return TransactionEvent(
        transaction_id=_rid("tx_"),
        event_time=_utc_now_iso(),
        customer_id=_rid("cust_", 8),
        amount=float(amount),
        currency="USD",
        item_count=int(item_count),
        payment_method=payment_method,
        country=country,
    )


class _Stop:
    stop = False


def _handle_stop(_sig: int, _frame: Optional[object]) -> None:
    _Stop.stop = True


def main() -> int:
    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or str(s.get("kafka.bootstrap_servers"))
    topic = os.getenv("KAFKA_TOPIC") or str(s.get("kafka.topic"))
    rate_per_sec = float(os.getenv("PRODUCER_RATE_PER_SEC") or "5")

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=True).encode("utf-8"),
        linger_ms=50,
        acks="all",
        retries=5,
    )
    logger.info(
        "Kafka producer started topic=%s bootstrap=%s rate_per_sec=%s",
        topic,
        bootstrap,
        rate_per_sec,
    )

    sleep_s = max(0.01, 1.0 / max(rate_per_sec, 0.1))
    while not _Stop.stop:
        ev = make_event()
        producer.send(topic, asdict(ev))
        time.sleep(sleep_s)

    producer.flush(10)
    producer.close(10)
    logger.info("Kafka producer stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
