from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from config.logging import configure_logging
from config.settings import load_settings
from storage.s3_client import S3Location, ensure_buckets, env_s3_buckets, upload_file

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ModelMeta:
    trained_at: str
    dt: str
    threshold_amount: float
    feature_columns: Dict[str, List[str]]
    label_column: str


def _load_latest_dt(s) -> str:
    state_path = s.resolve_path("storage/local/metadata/processed_state.json")
    if state_path.exists():
        import json

        state = json.loads(state_path.read_text(encoding="utf-8"))
        if isinstance(state, dict) and state.get("last_processed_dt"):
            return str(state["last_processed_dt"])
    return datetime.now(timezone.utc).date().isoformat()


def _read_processed_transactions(processed_root: Path, dt: Optional[str]) -> pd.DataFrame:
    base = processed_root / "transactions"
    if not base.exists():
        raise FileNotFoundError(f"Processed dataset not found: {base}")

    df = pd.read_parquet(base)
    if dt:
        df = df[df["dt"].astype(str) == str(dt)]
    if df.empty:
        raise ValueError("No processed rows available for training")
    return df


def train_and_save(dt: Optional[str] = None) -> Path:
    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    processed_root = s.resolve_path(
        str(s.get("spark.local_mirror.processed_dir"))
    )  # local demo path
    threshold = float(s.get("ml.threshold_amount", 200.0))
    feature_cols = dict(s.get("ml.feature_columns", {}))
    label_col = str(s.get("ml.label_column", "is_high_value"))

    resolved_dt = dt or _load_latest_dt(s)
    df = _read_processed_transactions(processed_root, resolved_dt)

    df[label_col] = (df["amount"].astype(float) >= threshold).astype(int)

    numeric = list(feature_cols.get("numeric", ["amount", "item_count"]))
    categorical = list(feature_cols.get("categorical", ["payment_method", "country"]))

    X = df[numeric + categorical]
    y = df[label_col]

    pre = ColumnTransformer(
        transformers=[
            ("num", Pipeline([("scaler", StandardScaler(with_mean=False))]), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )
    model = LogisticRegression(max_iter=500)
    pipe = Pipeline([("pre", pre), ("model", model)])

    logger.info("Training model on %d rows (dt=%s)", len(df), resolved_dt)
    pipe.fit(X, y)

    model_path = s.resolve_path(
        str(s.get("paths.model_path", "storage/local/artifacts/model.joblib"))
    )
    model_path.parent.mkdir(parents=True, exist_ok=True)

    meta = ModelMeta(
        trained_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        dt=str(resolved_dt),
        threshold_amount=threshold,
        feature_columns={"numeric": numeric, "categorical": categorical},
        label_column=label_col,
    )
    joblib.dump({"pipeline": pipe, "meta": meta.__dict__}, model_path)
    logger.info("Model saved: %s", model_path)

    # Upload model artifact to S3 models bucket (optional but enabled by default in docker-compose)
    try:
        buckets = env_s3_buckets()
        ensure_buckets(buckets.values())
        key = f"models/dt={resolved_dt}/model.joblib"
        upload_file(model_path, S3Location(bucket=buckets["models"], key=key))
    except Exception as e:
        logger.warning("Model upload to S3 skipped/failed (local run still OK): %s", e)

    return model_path


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Train an ML model from processed Parquet and save artifact."
    )
    parser.add_argument(
        "--dt", default=None, help="Partition date (yyyy-mm-dd). Defaults to last processed."
    )
    args = parser.parse_args(argv)
    train_and_save(dt=args.dt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
