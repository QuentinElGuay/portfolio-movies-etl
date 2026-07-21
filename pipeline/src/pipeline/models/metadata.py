from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel


METADATA_FILENAME = 'metadata.json'


class IngestionStatus(StrEnum):
    """Endpoint download status."""

    SUCCESS = 'success'
    PARTIAL = 'partial'
    FAILED = 'failed'


class IngestionMetadata(BaseModel):
    run_id: str
    dataset: str
    layer: str
    endpoint: str
    snapshot_date: date

    started_at: datetime
    finished_at: datetime

    status: IngestionStatus

    records_valid: int
    records_invalid: int
    unexpected_fields: set[str]

    schema_version: int = 1
