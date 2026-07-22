from abc import ABC
from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel


INGESTION_METADATA_FILENAME = 'metadata.json'


class IngestionStatus(StrEnum):
    """Endpoint download status."""

    SUCCESS = 'success'
    PARTIAL = 'partial'
    FAILED = 'failed'


class DatasetMetadata(BaseModel, ABC):
    run_id: str
    dataset: str
    layer: str
    snapshot_date: date


class IngestionMetadata(DatasetMetadata):
    run_id: str
    dataset: str
    layer: str
    snapshot_date: date
    endpoint: str

    started_at: datetime
    finished_at: datetime

    status: IngestionStatus

    records_valid: int
    records_invalid: int
    unexpected_fields: set[str]

    schema_version: int = 1
