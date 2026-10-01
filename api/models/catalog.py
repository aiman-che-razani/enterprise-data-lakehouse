"""Pydantic v2 response models for the data catalog endpoints."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class TableMetadataResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    table_name: str  # layer-qualified, e.g. "silver.orders"
    layer: str
    domain: str
    location: str | None = None
    format: str
    row_count: int | None = None
    size_bytes: int | None = None
    registered_at: str | None = None
    last_updated: str | None = None
    description: str | None = None


class ColumnMetadataResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    column_name: str
    data_type: str
    is_nullable: bool
    is_pii: bool = False
    description: str | None = None
    ordinal_position: int | None = None


class LineageNodeResponse(BaseModel):
    table: str
    depth: int


class LineageResponse(BaseModel):
    table_name: str
    direction: str
    nodes: list[LineageNodeResponse]
