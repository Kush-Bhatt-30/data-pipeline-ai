from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import joblib
import pandas as pd

from config.logging import configure_logging
from config.settings import load_settings
from storage.s3_client import ensure_buckets, env_s3_buckets, upload_directory

logger = logging.getLogger(__name__)


def _load_latest_dt(s) -> str:
    state_path = s.resolve_path("storage/local/metadata/processed_state.json")
    if state_path.exists():
        import json

        state = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(state, dict) and state.get("last_processed_dt"):
            return str(state["last_processed_dt"])
    return datetime.now(timezone.utc).date().isoformat()


def _load_model(model_path: Path):
    if not model_path.exists():
        raise FileNotFoundError(f"Model artifact not found: {model_path}")
    blob = joblib.load(model_path)
    if not isinstance(blob, dict) or "pipeline" not in blob:
        raise ValueError("Invalid model artifact format")
    return blob["pipeline"], blob.get("meta", {})


def batch_predict(dt: Optional[str] = None) -> Path:
    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    resolved_dt = dt or _load_latest_dt(s)

    processed_root = s.resolve_path(str(s.get("spark.local_mirror.processed_dir")))
    base = processed_root / "transactions"
    if not base.exists():
        raise FileNotFoundError(f"Processed dataset not found: {base}")

    df = pd.read_parquet(base)
    df = df[df["dt"].astype(str) == str(resolved_dt)]
    if df.empty:
        raise ValueError(f"No processed rows found for dt={resolved_dt}")

    model_path = s.resolve_path(
        str(s.get("paths.model_path", "storage/local/artifacts/model.joblib"))
    )
    pipe, meta = _load_model(model_path)

    feature_cols = meta.get("feature_columns") or s.get("ml.feature_columns")
    numeric = list((feature_cols or {}).get("numeric", ["amount", "item_count"]))
    categorical = list((feature_cols or {}).get("categorical", ["payment_method", "country"]))

    X = df[numeric + categorical]
    proba = pipe.predict_proba(X)[:, 1]

    out = df[
        [
            "transaction_id",
            "event_time",
            "customer_id",
            "amount",
            "currency",
            "item_count",
            "payment_method",
            "country",
            "dt",
        ]
    ].copy()
    out["high_value_score"] = proba.astype(float)
    out["pred_is_high_value"] = (out["high_value_score"] >= 0.5).astype(int)

    bi_root = s.resolve_path(str(s.get("spark.local_mirror.bi_dir")))
    out_dir = bi_root / "predictions" / f"dt={resolved_dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "predictions.parquet"
    out.to_parquet(out_path, index=False)
    logger.info("Batch predictions written: %s", out_path)

    # Optional upload to S3 for BI tools that prefer object storage
    try:
        buckets = env_s3_buckets()
        ensure_buckets(buckets.values())
        upload_directory(out_dir, buckets["processed"], f"bi/predictions/dt={resolved_dt}/")
    except Exception as e:
        logger.warning("Prediction upload to S3 skipped/failed (local run still OK): %s", e)

    return out_path


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Batch prediction job that writes BI-ready outputs."
    )
    parser.add_argument(
        "--dt", default=None, help="Partition date (yyyy-mm-dd). Defaults to last processed."
    )
    args = parser.parse_args(argv)
    batch_predict(dt=args.dt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
