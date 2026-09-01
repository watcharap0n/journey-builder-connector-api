from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class StandardizationRunView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    run_id: uuid.UUID
    dataset_id: int
    workspace_id: uuid.UUID | None
    trigger_type: str
    run_status: str
    discovered_indexes: int = Field(ge=0)
    queued_indexes: int = Field(ge=0)
    skipped_indexes: int = Field(ge=0)
    source_records: int = Field(ge=0)
    new_records: int = Field(ge=0)
    duplicate_records: int = Field(ge=0)
    rejected_records: int = Field(ge=0)
    quarantined_records: int = Field(ge=0)
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_code: str | None = None
    error_message: str | None = None


class SourcePartitionInventory(BaseModel):
    source_index: str
    baseline_cutoff_source_id: str | None = None
    version_cutoff_ingest_id: int | None = Field(default=None, ge=0)

