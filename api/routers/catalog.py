"""
Data catalog endpoints — table registry, column metadata, lineage.

Tables are addressed by their layer-qualified name (``silver.orders``, ``gold.fact_sales``).

Routes:
  GET /catalog/tables             — all registered Delta tables (paginated)
  GET /catalog/tables/{table}     — single table metadata
  GET /catalog/columns/{table}    — column list for a table
  GET /catalog/lineage/{table}    — upstream / downstream tables (table-level lineage)
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status

from api.dependencies import AuthDep, DbDep, require_scope
from api.models.catalog import (
    ColumnMetadataResponse,
    LineageNodeResponse,
    LineageResponse,
    TableMetadataResponse,
)
from api.repositories.catalog_repo import CatalogRepository
from src.lineage.models import walk

router = APIRouter(
    tags=["catalog"],
    dependencies=[Depends(require_scope("read:catalog"))],
)

_TableName = Annotated[str, Path(description="Layer-qualified table name, e.g. silver.orders")]


def _not_found(table_name: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Table '{table_name}' not found in catalog",
    )


@router.get(
    "/tables",
    response_model=list[TableMetadataResponse],
    summary="List registered Delta tables in the catalog",
)
def list_tables(
    conn: DbDep,
    _auth: AuthDep,
    layer: Annotated[
        str | None, Query(pattern="^(bronze|silver|gold)$", description="Filter by layer")
    ] = None,
    domain: Annotated[str | None, Query(description="Filter by domain name")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[TableMetadataResponse]:
    rows = CatalogRepository(conn).list_tables(
        layer=layer, domain=domain, limit=limit, offset=offset
    )
    return [TableMetadataResponse(**r) for r in rows]


@router.get(
    "/tables/{table_name}",
    response_model=TableMetadataResponse,
    summary="Single table metadata by layer-qualified name",
)
def get_table(table_name: _TableName, conn: DbDep, _auth: AuthDep) -> TableMetadataResponse:
    row = CatalogRepository(conn).get_table(table_name)
    if row is None:
        raise _not_found(table_name)
    return TableMetadataResponse(**row)


@router.get(
    "/columns/{table_name}",
    response_model=list[ColumnMetadataResponse],
    summary="Column metadata for a registered table",
)
def list_columns(
    table_name: _TableName, conn: DbDep, _auth: AuthDep
) -> list[ColumnMetadataResponse]:
    repo = CatalogRepository(conn)
    if not repo.table_exists(table_name):
        raise _not_found(table_name)
    return [ColumnMetadataResponse(**r) for r in repo.list_columns(table_name)]


@router.get(
    "/lineage/{table_name}",
    response_model=LineageResponse,
    summary="Upstream or downstream lineage for a table (e.g. gold.fact_sales)",
)
def get_lineage(
    table_name: _TableName,
    conn: DbDep,
    _auth: AuthDep,
    direction: Annotated[str, Query(pattern="^(upstream|downstream)$")] = "upstream",
    max_depth: Annotated[int | None, Query(ge=1, le=10)] = None,
) -> LineageResponse:
    edges = CatalogRepository(conn).lineage_edges()
    known = {e.source_table for e in edges} | {e.target_table for e in edges}
    if table_name not in known:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No lineage recorded for '{table_name}'",
        )
    nodes = walk(edges, table_name, direction, max_depth)  # type: ignore[arg-type]
    return LineageResponse(
        table_name=table_name,
        direction=direction,
        nodes=[LineageNodeResponse(table=n.table, depth=n.depth) for n in nodes],
    )
