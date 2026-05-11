from __future__ import annotations

import hashlib
import logging
import re

import boto3

from core.config import settings

log = logging.getLogger(__name__)


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


_CONTENT_TYPES: dict[str, str] = {
    ".pdf":  "application/pdf",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xls":  "application/vnd.ms-excel",
    ".doc":  "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".jpg":  "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png":  "image/png",
    ".zip":  "application/zip",
}


def _content_type_for(filename: str) -> str:
    import os
    ext = os.path.splitext(filename)[1].lower()
    return _CONTENT_TYPES.get(ext, "application/octet-stream")


def upload_document(project_key: str, filename: str, data: bytes, dry_run: bool = False) -> str | None:
    """Upload bytes to S3. Returns the S3 key on success, None on failure.

    If dry_run=True, skips actual upload and returns the key immediately.
    """
    s3_key = build_s3_key(project_key, filename)

    if dry_run:
        return s3_key

    try:
        client = _get_client()
        client.put_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=s3_key,
            Body=data,
            ContentType=_content_type_for(filename),
        )
        return s3_key
    except Exception as exc:
        log.error("S3 upload failed — skipping: %s (key=%s)", exc, s3_key)
        return None


def get_s3_url(s3_key: str) -> str:
    """Return the public CDN URL for a stored object.

    S3_BUCKET_NAME doubles as the public domain (e.g. docs.primetenders.com),
    so the URL is simply https://{S3_BUCKET_NAME}/{s3_key}.
    """
    return f"https://{settings.S3_BUCKET_NAME}/{s3_key}"
