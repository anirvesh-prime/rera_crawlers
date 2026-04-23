from __future__ import annotations

import hashlib
import re

import boto3
from botocore.exceptions import ClientError

from core.config import settings


def _get_client():
    return boto3.client(
        "s3",
        region_name=settings.AWS_REGION,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    )


def compute_md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w.\-]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")[:200]


def build_s3_key(project_key: str, filename: str) -> str:
    return f"{project_key}/{sanitize_filename(filename)}"


def upload_document(project_key: str, filename: str, data: bytes, dry_run: bool = False) -> str:
    """
    Upload bytes to S3. Returns the S3 key.
    If dry_run=True, skips actual upload (useful for local testing without real S3).
    """
    s3_key = build_s3_key(project_key, filename)

    if dry_run:
        return s3_key

    client = _get_client()
    client.put_object(
        Bucket=settings.S3_BUCKET_NAME,
        Key=s3_key,
        Body=data,
        ContentType="application/pdf",
    )
    return s3_key


def document_exists(project_key: str, filename: str) -> bool:
    s3_key = build_s3_key(project_key, filename)
    try:
        _get_client().head_object(Bucket=settings.S3_BUCKET_NAME, Key=s3_key)
        return True
    except ClientError:
        return False


def get_s3_url(s3_key: str) -> str:
    """Return the public URL for a stored S3 object.

    When CDN_BASE_URL is set (e.g. "https://docs.primetenders.com") the URL
    will be  {CDN_BASE_URL}/{s3_key}.  Otherwise falls back to the raw S3
    virtual-hosted URL so local/dev runs still produce something usable.
    """
    if settings.CDN_BASE_URL:
        base = settings.CDN_BASE_URL.rstrip("/")
        return f"{base}/{s3_key}"
    return f"https://{settings.S3_BUCKET_NAME}.s3.{settings.AWS_REGION}.amazonaws.com/{s3_key}"
