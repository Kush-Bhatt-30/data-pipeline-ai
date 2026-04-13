from __future__ import annotations

import json
import logging
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config.settings import load_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3Location:
    bucket: str
    key: str

    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key.lstrip('/')}"


def _client_config() -> Config:
    # Slightly higher timeouts for LocalStack on some machines.
    return Config(
        retries={"max_attempts": 5, "mode": "standard"},
        connect_timeout=5,
        read_timeout=30,
    )


def get_s3_client() -> Any:
    s = load_settings()
    endpoint_url = s.get("s3.endpoint_url")
    region = s.get("s3.region")
    access_key_id = s.get("s3.access_key_id")
    secret_access_key = s.get("s3.secret_access_key")

    session = boto3.session.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )

    return session.client("s3", endpoint_url=endpoint_url, config=_client_config())


def ensure_buckets(buckets: Iterable[str]) -> None:
    client = get_s3_client()
    s = load_settings()
    region = s.get("s3.region", "us-east-1")

    for b in buckets:
        try:
            client.head_bucket(Bucket=b)
            logger.info("S3 bucket exists: %s", b)
            continue
        except ClientError as e:
            code = (e.response or {}).get("Error", {}).get("Code")
            if code not in {"404", "NoSuchBucket", "NotFound"}:
                logger.warning("head_bucket failed for %s: %s", b, e)

        params: Dict[str, Any] = {"Bucket": b}
        # LocalStack ignores CreateBucketConfiguration, AWS needs it for non-us-east-1.
        if region and region != "us-east-1":
            params["CreateBucketConfiguration"] = {"LocationConstraint": region}
        client.create_bucket(**params)
        logger.info("Created S3 bucket: %s", b)


def put_json(loc: S3Location, payload: Any) -> None:
    client = get_s3_client()
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    client.put_object(
        Bucket=loc.bucket,
        Key=loc.key.lstrip("/"),
        Body=body,
        ContentType="application/json",
    )
    logger.info("Uploaded JSON to %s", loc.uri())


def put_bytes(loc: S3Location, body: bytes, content_type: Optional[str] = None) -> None:
    client = get_s3_client()
    ct = content_type or "application/octet-stream"
    client.put_object(Bucket=loc.bucket, Key=loc.key.lstrip("/"), Body=body, ContentType=ct)
    logger.info("Uploaded bytes to %s (%s)", loc.uri(), ct)


def download_to_path(loc: S3Location, dst: Path) -> Path:
    client = get_s3_client()
    dst.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(loc.bucket, loc.key.lstrip("/"), str(dst))
    logger.info("Downloaded %s -> %s", loc.uri(), dst)
    return dst


def list_keys(bucket: str, prefix: str) -> list[str]:
    client = get_s3_client()
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.lstrip("/")):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def upload_file(src: Path, loc: S3Location) -> None:
    client = get_s3_client()
    ct, _ = mimetypes.guess_type(str(src))
    kwargs: Dict[str, Any] = {}
    if ct:
        kwargs["ExtraArgs"] = {"ContentType": ct}
    client.upload_file(str(src), loc.bucket, loc.key.lstrip("/"), **kwargs)
    logger.info("Uploaded file %s -> %s", src, loc.uri())


def upload_directory(src_dir: Path, bucket: str, prefix: str) -> int:
    """
    Recursively uploads a directory to S3, preserving relative structure.
    Returns the number of uploaded files.
    """
    src_dir = src_dir.resolve()
    if not src_dir.exists():
        raise FileNotFoundError(str(src_dir))

    count = 0
    for p in src_dir.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(src_dir).as_posix()
        key = f"{prefix.rstrip('/')}/{rel}"
        upload_file(p, S3Location(bucket=bucket, key=key))
        count += 1
    return count


def mirror_to_local(raw_bytes: bytes, rel_path: str) -> Path:
    """
    Writes a local mirror copy (useful for local Spark runs).
    Controlled by config.paths.local_storage_root and config.spark.local_mirror.*.
    """
    s = load_settings()
    root = s.resolve_path(s.get("paths.local_storage_root", "storage/local"))
    target = root / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw_bytes)
    return target


def env_s3_buckets() -> Dict[str, str]:
    s = load_settings()
    return {
        "raw": os.getenv("S3_BUCKET_RAW") or str(s.get("s3.buckets.raw")),
        "processed": os.getenv("S3_BUCKET_PROCESSED") or str(s.get("s3.buckets.processed")),
        "warehouse": os.getenv("S3_BUCKET_WAREHOUSE") or str(s.get("s3.buckets.warehouse")),
        "models": os.getenv("S3_BUCKET_MODELS") or str(s.get("s3.buckets.models")),
    }
