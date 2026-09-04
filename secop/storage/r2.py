"""Almacén de objetos sobre Cloudflare R2 (API S3, vía boto3)."""

from __future__ import annotations

import logging

from botocore.config import Config
from botocore.exceptions import ClientError

from secop.config import Settings

logger = logging.getLogger(__name__)


class R2Store:
    """Implementación de :class:`~secop.storage.base.ObjectStore` para R2."""

    def __init__(self, settings: Settings) -> None:
        settings.require_r2()
        self._bucket = settings.r2_bucket
        self.label = f"r2://{self._bucket}"
        self._client = self._build_client(settings)

    @staticmethod
    def _build_client(settings: Settings):  # noqa: ANN205 - boto3 client no está tipado
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    def put_bytes(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self._bucket, Key=key, Body=data)
        logger.debug("PUT %s/%s (%d bytes)", self.label, key, len(data))

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                raise KeyError(key) from exc
            raise
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                return False
            raise
        return True

    def list_keys(self, prefix: str) -> list[str]:
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def copy(self, src_key: str, dst_key: str) -> None:
        self._client.copy_object(
            Bucket=self._bucket,
            Key=dst_key,
            CopySource={"Bucket": self._bucket, "Key": src_key},
        )
