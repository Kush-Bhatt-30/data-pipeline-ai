from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from kafka import KafkaConsumer

from config.logging import configure_logging
from config.settings import load_settings
from storage.s3_client import S3Location, ensure_buckets, env_s3_buckets, mirror_to_local, put_bytes

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FlushConfig:
    max_records: int = 500
    max_seconds: float = 5.0


class _Stop:
    stop = False


def _handle_stop(_sig: int, _frame: Optional[object]) -> None:
    _Stop.stop = True


def _dt_from_iso(ts: str) -> str:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return datetime.now(timezone.utc).date().isoformat()


def _jsonl_bytes(records: List[Dict[str, Any]]) -> bytes:
    lines = [json.dumps(r, ensure_ascii=True) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _flush(records: List[Dict[str, Any]], *, dataset: str, dt: str) -> None:
    if not records:
        return

    buckets = env_s3_buckets()
    ensure_buckets(buckets.values())

    key = f"{dataset}/dt={dt}/source=kafka/{dataset}_{int(time.time())}.jsonl"
    body = _jsonl_bytes(records)

    put_bytes(S3Location(bucket=buckets["raw"], key=key), body, content_type="application/x-ndjson")
    mirror_rel = f"raw_mirror/{key}"
    local_path = mirror_to_local(body, mirror_rel)
    logger.info("Flushed %d records to S3 and local mirror (%s)", len(records), local_path)


def main() -> int:
    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or str(s.get("kafka.bootstrap_servers"))
    topic = os.getenv("KAFKA_TOPIC") or str(s.get("kafka.topic"))
    group_id = os.getenv("KAFKA_GROUP_ID") or str(s.get("kafka.group_id"))

    flush_max_records = int(os.getenv("CONSUMER_FLUSH_MAX_RECORDS") or "500")
    flush_max_seconds = float(os.getenv("CONSUMER_FLUSH_MAX_SECONDS") or "5")
    flush_cfg = FlushConfig(max_records=flush_max_records, max_seconds=flush_max_seconds)

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

    logger.info(
        "Kafka consumer started topic=%s bootstrap=%s group_id=%s", topic, bootstrap, group_id
    )
    buf: List[Dict[str, Any]] = []
    last_flush = time.time()

    try:
        while not _Stop.stop:
            for msg in consumer:
                if _Stop.stop:
                    break
                if not isinstance(msg.value, dict):
                    continue
                buf.append(msg.value)

                now = time.time()
                if len(buf) >= flush_cfg.max_records or (now - last_flush) >= flush_cfg.max_seconds:
                    dt = _dt_from_iso(str(buf[-1].get("event_time", "")))
                    _flush(buf, dataset="transactions", dt=dt)
                    buf = []
                    last_flush = now
            # loop continues; consumer_timeout_ms gives us periodic wakeups to honor stop
    finally:
        if buf:
            dt = _dt_from_iso(str(buf[-1].get("event_time", "")))
            _flush(buf, dataset="transactions", dt=dt)
        consumer.close()
        logger.info("Kafka consumer stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
