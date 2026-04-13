from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


def _deep_set(d: Dict[str, Any], keys: list[str], value: Any) -> None:
    cur: Dict[str, Any] = d
    for k in keys[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    cur[keys[-1]] = value


def _coerce_env_value(v: str) -> Any:
    vl = v.strip()
    if vl.lower() in {"true", "false"}:
        return vl.lower() == "true"
    try:
        if "." in vl:
            return float(vl)
        return int(vl)
    except ValueError:
        return vl


def _apply_env_overrides(cfg: Dict[str, Any], prefix: str = "DPA__") -> Dict[str, Any]:
    """
    Overrides YAML config via env vars like:
      DPA__S3__ENDPOINT_URL=http://localhost:4566
      DPA__KAFKA__BOOTSTRAP_SERVERS=kafka:9092
    """
    out = copy.deepcopy(cfg)
    for k, v in os.environ.items():
        if not k.startswith(prefix):
            continue
        path = k[len(prefix) :].lower().split("__")
        _deep_set(out, path, _coerce_env_value(v))
    return out


@dataclass(frozen=True)
class Settings:
    raw: Dict[str, Any]

    @property
    def env(self) -> str:
        return str(self.raw.get("app", {}).get("env", "local"))

    def get(self, path: str, default: Any = None) -> Any:
        cur: Any = self.raw
        for p in path.split("."):
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    def repo_root(self) -> Path:
        # config/ is always one level below repo root
        return Path(__file__).resolve().parents[1]

    def resolve_path(self, rel: str) -> Path:
        return (self.repo_root() / rel).resolve()


_CACHED: Optional[Settings] = None


def load_settings(config_path: Optional[str] = None) -> Settings:
    global _CACHED
    if _CACHED is not None:
        return _CACHED

    path = Path(config_path) if config_path else Path(__file__).resolve().parent / "config.yaml"
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    cfg = _apply_env_overrides(cfg)
    _CACHED = Settings(raw=cfg)
    return _CACHED
