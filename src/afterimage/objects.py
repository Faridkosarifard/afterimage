"""Noncurrent object versions in versioned S3 / MinIO buckets.

When bucket versioning is enabled, deleting or overwriting an object only hides it:
the previous bytes stay retrievable by version id until a lifecycle rule or an
explicit ``DeleteObject(VersionId=...)`` removes them. Iceberg maintenance
(expire snapshots, remove orphan files) never touches noncurrent versions.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from afterimage.inspect_files import FileInspector, Inspection, file_kind
from afterimage.models import Confidence
from afterimage.storage import S3Location

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)

#: Do not download noncurrent versions larger than this by default (bytes).
DEFAULT_MAX_DOWNLOAD_BYTES = 512 * 1024 * 1024


@dataclass(frozen=True)
class ObjectVersion:
    bucket: str
    key: str
    version_id: str
    size: int
    is_delete_marker: bool = False


@dataclass(frozen=True)
class VersionHit:
    """A noncurrent version that reveals (or may reveal) the subject. Holds the raw key."""

    version: ObjectVersion
    confidence: Confidence
    inspection: Inspection

    @property
    def uri(self) -> str:
        return f"s3://{self.version.bucket}/{self.version.key}"


def s3_client_from_properties(properties: dict[str, str], overrides: dict[str, str] | None = None) -> S3Client:
    """Build a boto3 S3 client from PyIceberg-style ``s3.*`` properties."""
    import boto3
    from botocore.config import Config

    props = {**properties, **(overrides or {})}
    endpoint = props.get("s3.endpoint")
    kwargs: dict[str, Any] = {}
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if props.get("s3.access-key-id"):
        kwargs["aws_access_key_id"] = props["s3.access-key-id"]
        kwargs["aws_secret_access_key"] = props.get("s3.secret-access-key")
    if props.get("s3.session-token"):
        kwargs["aws_session_token"] = props["s3.session-token"]
    region = props.get("s3.region") or props.get("client.region")
    if region:
        kwargs["region_name"] = region
    path_style = endpoint is not None and props.get("s3.force-virtual-addressing", "false").lower() != "true"
    kwargs["config"] = Config(s3={"addressing_style": "path" if path_style else "auto"})
    return boto3.client("s3", **kwargs)


def iter_noncurrent_versions(client: S3Client, location: S3Location) -> Iterator[ObjectVersion]:
    """Yield every noncurrent version (and delete marker) under the location prefix."""
    prefix = location.prefix.rstrip("/") + "/" if location.prefix else ""
    paginator = client.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=location.bucket, Prefix=prefix):
        for version in page.get("Versions", []):
            if version.get("IsLatest"):
                continue
            yield ObjectVersion(
                bucket=location.bucket,
                key=version["Key"],
                version_id=version["VersionId"],
                size=int(version.get("Size", 0)),
            )
        for marker in page.get("DeleteMarkers", []):
            yield ObjectVersion(
                bucket=location.bucket,
                key=marker["Key"],
                version_id=marker["VersionId"],
                size=0,
                is_delete_marker=True,
            )


def bucket_versioning_status(client: S3Client, bucket: str) -> str:
    """Return ``Enabled``, ``Suspended`` or ``Disabled`` (never enabled)."""
    response = client.get_bucket_versioning(Bucket=bucket)
    return str(response.get("Status", "Disabled"))


class ObjectVersionScanner:
    def __init__(
        self,
        client: S3Client,
        inspector: FileInspector,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    ) -> None:
        self.client = client
        self.inspector = inspector
        self.max_download_bytes = max_download_bytes

    def scan(self, location: S3Location) -> list[VersionHit]:
        hits: list[VersionHit] = []
        for version in iter_noncurrent_versions(self.client, location):
            if version.is_delete_marker or file_kind(version.key) == "other":
                continue
            hit = self._inspect(version)
            if hit is not None:
                hits.append(hit)
        return hits

    def _inspect(self, version: ObjectVersion) -> VersionHit | None:
        if version.size > self.max_download_bytes:
            inspection = Inspection(parsed=False)
            inspection.add(
                None,
                Confidence.POSSIBLE,
                f"noncurrent version larger than the {self.max_download_bytes} byte download limit",
            )
            return VersionHit(version, Confidence.POSSIBLE, inspection)
        response = self.client.get_object(Bucket=version.bucket, Key=version.key, VersionId=version.version_id)
        data = response["Body"].read()
        found = self.inspector.inspect_bytes(version.key, data)
        if found is None or found.confidence is None:
            return None
        return VersionHit(version, found.confidence, found)


@dataclass(frozen=True)
class PurgeOutcome:
    hit: VersionHit
    deleted: bool
    error: str | None = None


def purge_versions(client: S3Client, hits: list[VersionHit], *, confirm: bool) -> list[PurgeOutcome]:
    """Permanently delete the given noncurrent versions. Does nothing unless ``confirm`` is True.

    Deleting a specific ``VersionId`` cannot be undone. Buckets with Object Lock or MFA
    delete will refuse; those failures are reported per version and never retried.
    """
    outcomes: list[PurgeOutcome] = []
    for hit in hits:
        if not confirm:
            outcomes.append(PurgeOutcome(hit, deleted=False))
            continue
        try:
            client.delete_object(Bucket=hit.version.bucket, Key=hit.version.key, VersionId=hit.version.version_id)
            outcomes.append(PurgeOutcome(hit, deleted=True))
        except Exception as exc:  # botocore ClientError and friends
            outcomes.append(PurgeOutcome(hit, deleted=False, error=type(exc).__name__ + ": " + str(exc)))
    return outcomes
