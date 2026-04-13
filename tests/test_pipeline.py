from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd


def _reset_settings_cache() -> None:
    import config.settings as cs

    cs._CACHED = None  # type: ignore[attr-defined]


def test_settings_env_override() -> None:
    os.environ["DPA__APP__LOG_LEVEL"] = "DEBUG"
    _reset_settings_cache()
    from config.settings import load_settings

    s = load_settings()
    assert s.get("app.log_level") == "DEBUG"


def test_model_training_and_batch_prediction(tmp_path: Path) -> None:
    processed_root = tmp_path / "processed"
    artifacts_root = tmp_path / "artifacts"
    bi_root = tmp_path / "bi"
    meta_root = tmp_path / "metadata"
    (processed_root / "transactions").mkdir(parents=True, exist_ok=True)
    artifacts_root.mkdir(parents=True, exist_ok=True)
    bi_root.mkdir(parents=True, exist_ok=True)
    meta_root.mkdir(parents=True, exist_ok=True)

    dt = "2026-04-10"

    # Minimal processed dataset schema as written by Spark job
    df = pd.DataFrame(
        [
            {
                "transaction_id": "tx_a",
                "event_time": "2026-04-10T08:00:00+00:00",
                "customer_id": "cust_1",
                "amount": 250.0,
                "currency": "USD",
                "item_count": 2,
                "payment_method": "card",
                "country": "US",
                "dt": dt,
            },
            {
                "transaction_id": "tx_b",
                "event_time": "2026-04-10T09:00:00+00:00",
                "customer_id": "cust_2",
                "amount": 40.0,
                "currency": "USD",
                "item_count": 1,
                "payment_method": "wallet",
                "country": "IN",
                "dt": dt,
            },
        ]
    )
    df.to_parquet(processed_root / "transactions" / "part-000.parquet", index=False)

    # Pretend spark processed dt already
    state = {"last_processed_dt": dt}
    (meta_root / "processed_state.json").write_text(json.dumps(state), encoding="utf-8")

    os.environ["DPA__SPARK__LOCAL_MIRROR__PROCESSED_DIR"] = str(processed_root)
    os.environ["DPA__SPARK__LOCAL_MIRROR__BI_DIR"] = str(bi_root)
    os.environ["DPA__PATHS__MODEL_PATH"] = str(artifacts_root / "model.joblib")
    os.environ["DPA__APP__LOG_LEVEL"] = "WARNING"

    # Keep state file in our temp dir by overriding local_storage_root and writing it in the expected place
    os.environ["DPA__PATHS__LOCAL_STORAGE_ROOT"] = str(tmp_path)
    (tmp_path / "metadata").mkdir(parents=True, exist_ok=True)
    (tmp_path / "metadata" / "processed_state.json").write_text(json.dumps(state), encoding="utf-8")

    _reset_settings_cache()
    from ai.model_training import train_and_save
    from ai.prediction import batch_predict

    model_path = train_and_save(dt=dt)
    assert model_path.exists()

    pred_path = batch_predict(dt=dt)
    assert pred_path.exists()

    out = pd.read_parquet(pred_path)
    assert "high_value_score" in out.columns
    assert len(out) == 2


def test_local_mirror_write(tmp_path: Path) -> None:
    os.environ["DPA__PATHS__LOCAL_STORAGE_ROOT"] = str(tmp_path)
    os.environ["DPA__APP__LOG_LEVEL"] = "WARNING"
    _reset_settings_cache()

    from storage.s3_client import mirror_to_local

    p = mirror_to_local(b"hello\n", "raw_mirror/test/hello.txt")
    assert p.exists()
    assert p.read_bytes() == b"hello\n"
