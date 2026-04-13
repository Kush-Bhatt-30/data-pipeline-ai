from __future__ import annotations

import argparse
import csv
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

from config.logging import configure_logging
from config.settings import load_settings
from storage.s3_client import S3Location, ensure_buckets, env_s3_buckets, mirror_to_local, put_bytes

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    event_time: str  # ISO-8601
    customer_id: str
    amount: float
    currency: str
    item_count: int
    payment_method: str
    country: str


REQUIRED_FIELDS = {
    "transaction_id",
    "event_time",
    "customer_id",
    "amount",
    "currency",
    "item_count",
    "payment_method",
    "country",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _validate_tx(d: Dict[str, Any]) -> Transaction:
    missing = REQUIRED_FIELDS - set(d.keys())
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")

    return Transaction(
        transaction_id=str(d["transaction_id"]),
        event_time=str(d["event_time"]),
        customer_id=str(d["customer_id"]),
        amount=float(d["amount"]),
        currency=str(d["currency"]),
        item_count=int(d["item_count"]),
        payment_method=str(d["payment_method"]),
        country=str(d["country"]),
    )


def _jsonl_bytes(records: Iterable[Dict[str, Any]]) -> bytes:
    lines = [json.dumps(r, ensure_ascii=True) for r in records]
    return ("\n".join(lines) + "\n").encode("utf-8")


def fetch_from_rest_api(url: str, timeout_s: int = 10) -> List[Transaction]:
    logger.info("Fetching transactions from REST source: %s", url)
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    payload = r.json()
    if isinstance(payload, dict) and "data" in payload:
        items = payload["data"]
    else:
        items = payload
    if not isinstance(items, list):
        raise ValueError("REST source did not return a list of transactions")

    out: List[Transaction] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        out.append(_validate_tx(it))
    logger.info("Fetched %d transactions from REST source", len(out))
    return out


def read_transactions_from_csv(path: Path) -> List[Transaction]:
    logger.info("Reading CSV batch file: %s", path)
    out: List[Transaction] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out.append(_validate_tx(row))
    logger.info("Read %d transactions from %s", len(out), path.name)
    return out


def read_transactions_from_jsonl(path: Path) -> List[Transaction]:
    logger.info("Reading JSONL batch file: %s", path)
    out: List[Transaction] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(_validate_tx(json.loads(line)))
    logger.info("Read %d transactions from %s", len(out), path.name)
    return out


def upload_raw_transactions(
    txs: List[Transaction],
    *,
    dt: str,
    source: str,
    dataset: str = "transactions",
) -> None:
    s = load_settings()
    buckets = env_s3_buckets()
    ensure_buckets(buckets.values())

    ts = int(time.time())
    key = f"{dataset}/dt={dt}/source={source}/{dataset}_{ts}.jsonl"
    body = _jsonl_bytes([asdict(t) for t in txs])

    put_bytes(S3Location(bucket=buckets["raw"], key=key), body, content_type="application/x-ndjson")

    # Local mirror (for local Spark I/O mode)
    mirror_rel = f"raw_mirror/{key}"
    local_path = mirror_to_local(body, mirror_rel)
    logger.info("Local mirror written: %s", local_path)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Batch ingestion into raw S3 zone (REST + files).")
    parser.add_argument("--mode", choices=["api", "files"], required=True)
    parser.add_argument("--dt", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--api-url", default=None, help="Overrides config/env REST source URL")
    args = parser.parse_args(argv)

    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    dt = str(args.dt)

    if args.mode == "api":
        default_url = str(
            s.get("sources.rest_api_url", "http://api:8000/source/transactions?count=200")
        )
        url = args.api_url or default_url
        txs = fetch_from_rest_api(url)
        upload_raw_transactions(txs, dt=dt, source="api")
        return 0

    # files mode
    sample_dir = s.resolve_path(str(s.get("sources.batch_files_dir", "storage/local/sample_data")))
    csv_path = sample_dir / "transactions.csv"
    jsonl_path = sample_dir / "transactions.jsonl"

    txs: List[Transaction] = []
    if csv_path.exists():
        txs.extend(read_transactions_from_csv(csv_path))
    if jsonl_path.exists():
        txs.extend(read_transactions_from_jsonl(jsonl_path))

    if not txs:
        raise FileNotFoundError(
            f"No batch files found in {sample_dir} (expected transactions.csv and/or transactions.jsonl)"
        )

    upload_raw_transactions(txs, dt=dt, source="files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
