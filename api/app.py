from __future__ import annotations

import logging
import random
import string
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config.logging import configure_logging
from config.settings import load_settings

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _rid(prefix: str, n: int = 10) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


class PredictRequest(BaseModel):
    amount: float = Field(..., ge=0)
    item_count: int = Field(..., ge=1)
    payment_method: str
    country: str


class PredictResponse(BaseModel):
    high_value_score: float
    pred_is_high_value: int
    model_dt: Optional[str] = None


class SourceTransaction(BaseModel):
    transaction_id: str
    event_time: str
    customer_id: str
    amount: float
    currency: str
    item_count: int
    payment_method: str
    country: str


_MODEL_CACHE: Dict[str, Dict[str, Any]] = {}


def _load_model(model_path: Path) -> Dict[str, Any]:
    """
    Loads and caches the trained machine learning pipeline artifact.
    
    Implementing an in-memory cache boundary ensures high-concurrency 
    API requests do not incur disk I/O bottlenecks.
    
    Args:
        model_path (Path): Absolute path to the serialized joblib artifact.

    Raises:
        FileNotFoundError: If the model artifact has not been published yet.
        ValueError: If the loaded artifact violates the expected pipeline schema.

    Returns:
        Dict[str, Any]: The operational ML model and associated lineage metadata.
    """
    if "model" in _MODEL_CACHE:
        return _MODEL_CACHE["model"]

    if not model_path.exists():
        raise FileNotFoundError(str(model_path))

    blob = joblib.load(model_path)
    if not isinstance(blob, dict) or "pipeline" not in blob:
        raise ValueError("Invalid model artifact format")

    _MODEL_CACHE["model"] = blob
    return blob


def create_app() -> FastAPI:
    """
    Application Factory for the prediction server.
    
    This factory initialization pattern maps routing, dependency injection, 
    and systemic logging protocols dynamically up-front.
    
    Returns:
        FastAPI: The initialized web server application hook.
    """
    s = load_settings()
    configure_logging(str(s.get("app.log_level", "INFO")))

    app = FastAPI(title="data-pipeline-ai", version="0.1.0")

    model_path = s.resolve_path(
        str(s.get("paths.model_path", "storage/local/artifacts/model.joblib"))
    )

    @app.get("/health")
    def health() -> Dict[str, Any]:
        return {"status": "ok", "time_utc": _utc_now_iso()}

    @app.get("/model/meta")
    def model_meta() -> Dict[str, Any]:
        try:
            blob = _load_model(model_path)
            return {"loaded": True, "meta": blob.get("meta", {})}
        except FileNotFoundError:
            return {"loaded": False, "meta": None, "error": f"model not found at {model_path}"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"model load error: {e}") from e

    @app.post("/predict", response_model=PredictResponse)
    def predict(req: PredictRequest) -> PredictResponse:
        try:
            blob = _load_model(model_path)
        except FileNotFoundError:
            raise HTTPException(
                status_code=503,
                detail="Model not trained yet. Run the pipeline to generate model.joblib.",
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"model load error: {e}") from e

        pipe = blob["pipeline"]
        meta = blob.get("meta", {}) or {}

        x = pd.DataFrame(
            [
                {
                    "amount": float(req.amount),
                    "item_count": int(req.item_count),
                    "payment_method": str(req.payment_method),
                    "country": str(req.country),
                }
            ]
        )
        proba = float(pipe.predict_proba(x)[0, 1])
        pred = int(proba >= 0.5)
        return PredictResponse(
            high_value_score=proba, pred_is_high_value=pred, model_dt=meta.get("dt")
        )

    @app.get("/source/transactions")
    def source_transactions(count: int = 100) -> Dict[str, Any]:
        """
        Local REST "source system" for demo ingestion.
        In production this would be an external REST API.
        """
        count = max(1, min(int(count), 1000))
        out = []
        for _ in range(count):
            payment_method = random.choice(["card", "upi", "cod", "wallet"])
            country = random.choice(["US", "IN", "GB", "DE", "SG", "AU"])
            amount = round(random.lognormvariate(4.3, 0.6), 2)
            item_count = max(1, int(random.gauss(2.2, 1.0)))
            out.append(
                SourceTransaction(
                    transaction_id=_rid("tx_"),
                    event_time=_utc_now_iso(),
                    customer_id=_rid("cust_", 8),
                    amount=float(amount),
                    currency="USD",
                    item_count=int(item_count),
                    payment_method=payment_method,
                    country=country,
                ).model_dump()
            )
        return {"data": out}

    return app


app = create_app()
