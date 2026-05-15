"""Pydantic v2 response models for the subset of the Lightdash REST API we read.

All Lightdash responses are wrapped as ``{"status": "ok", "results": ...}``; the
client unwraps ``results`` before handing the payload to these models. Every model
sets ``extra="ignore"`` so the connector keeps working when Lightdash adds new
fields on minor upgrades.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class _LightdashModel(BaseModel):
    """Common base — be lenient about unknown fields, allow alias-style population."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class LightdashUserRef(_LightdashModel):
    """User reference embedded on entities (``updatedByUser`` / ``createdByUser``).

    Older Lightdash builds don't expose ``email`` on this nested object — owner
    resolution falls back to ``userUuid`` when missing.
    """

    user_uuid: str = Field(alias="userUuid")
    first_name: str | None = Field(default=None, alias="firstName")
    last_name: str | None = Field(default=None, alias="lastName")
    email: str | None = None

    @property
    def display_name(self) -> str:
        parts = [p for p in (self.first_name, self.last_name) if p]
        return " ".join(parts) if parts else self.user_uuid


class LightdashOrganization(_LightdashModel):
    organization_uuid: str = Field(alias="organizationUuid")
    name: str
    created_at: datetime | None = Field(default=None, alias="createdAt")


class LightdashProjectSummary(_LightdashModel):
    """Item shape returned by ``GET /api/v1/org/projects``."""

    project_uuid: str = Field(alias="projectUuid")
    name: str
    type: str | None = None
    warehouse_type: str | None = Field(default=None, alias="warehouseType")
    created_at: datetime | None = Field(default=None, alias="createdAt")
    created_by_user_uuid: str | None = Field(default=None, alias="createdByUserUuid")
    created_by_user_name: str | None = Field(default=None, alias="createdByUserName")


class LightdashWarehouseConnection(_LightdashModel):
    type: str
    schema_: str | None = Field(default=None, alias="schema")
    host: str | None = None
    port: int | None = None
    database: str | None = None  # only on some warehouse types (snowflake/bigquery)


class LightdashProject(_LightdashModel):
    """Detailed shape returned by ``GET /api/v1/projects/{uuid}``."""

    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: str = Field(alias="organizationUuid")
    name: str
    type: str | None = None
    warehouse_connection: LightdashWarehouseConnection | None = Field(
        default=None, alias="warehouseConnection"
    )


class LightdashSpace(_LightdashModel):
    uuid: str
    name: str
    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: str = Field(alias="organizationUuid")
    slug: str | None = None
    path: str | None = None
    parent_space_uuid: str | None = Field(default=None, alias="parentSpaceUuid")
    chart_count: Any | None = Field(default=None, alias="chartCount")  # API returns str
    dashboard_count: Any | None = Field(default=None, alias="dashboardCount")


class LightdashChartSummary(_LightdashModel):
    """Item shape returned by ``GET /api/v1/projects/{uuid}/charts``.

    Note: this listing endpoint does NOT include ``tableName``; the source uses
    :class:`LightdashChart` (the saved-chart detail) to resolve upstream lineage.
    """

    uuid: str
    name: str
    description: str | None = None
    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: str | None = Field(default=None, alias="organizationUuid")
    space_uuid: str | None = Field(default=None, alias="spaceUuid")
    space_name: str | None = Field(default=None, alias="spaceName")
    dashboard_uuid: str | None = Field(default=None, alias="dashboardUuid")
    chart_type: str | None = Field(default=None, alias="chartType")
    chart_kind: str | None = Field(default=None, alias="chartKind")
    slug: str | None = None
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    updated_by_user: LightdashUserRef | None = Field(default=None, alias="updatedByUser")


class LightdashMetricQuery(_LightdashModel):
    """The chart's compiled query.

    ``dimensions`` and ``metrics`` are field-ID lists in the form
    ``<table>_<fieldName>`` (e.g. ``orders_enriched_order_id``). The source
    strips the table prefix and looks the suffix up in the matching Explore
    table's ``dimensions`` / ``metrics`` to resolve the underlying warehouse
    column for chart-level field lineage.
    """

    explore_name: str | None = Field(default=None, alias="exploreName")
    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)


class LightdashChart(LightdashChartSummary):
    """Detailed shape returned by ``GET /api/v1/saved/{uuid}``.

    Adds the warehouse-facing ``table_name`` and the ``metric_query.explore_name``
    that the source uses to look up join graph + sqlTable values from the
    matching Explore.
    """

    table_name: str | None = Field(default=None, alias="tableName")
    metric_query: LightdashMetricQuery | None = Field(default=None, alias="metricQuery")


class LightdashDashboardSummary(_LightdashModel):
    uuid: str
    name: str
    description: str | None = None
    project_uuid: str = Field(alias="projectUuid")
    organization_uuid: str | None = Field(default=None, alias="organizationUuid")
    space_uuid: str | None = Field(default=None, alias="spaceUuid")
    updated_at: datetime | None = Field(default=None, alias="updatedAt")
    updated_by_user: LightdashUserRef | None = Field(default=None, alias="updatedByUser")


class LightdashDashboardTileProperties(_LightdashModel):
    saved_chart_uuid: str | None = Field(default=None, alias="savedChartUuid")
    chart_name: str | None = Field(default=None, alias="chartName")
    chart_slug: str | None = Field(default=None, alias="chartSlug")


class LightdashDashboardTile(_LightdashModel):
    uuid: str
    type: str  # "saved_chart" / "markdown" / "loom"
    properties: LightdashDashboardTileProperties | None = None


class LightdashDashboard(LightdashDashboardSummary):
    tiles: list[LightdashDashboardTile] = Field(default_factory=list)


class LightdashFieldDef(_LightdashModel):
    """One Lightdash dimension or metric.

    ``name`` is the field identifier (e.g. ``order_id``); ``table`` matches a key
    in ``Explore.tables``; ``sql`` is the SQL fragment Lightdash compiles into the
    query — usually ``${TABLE}.<column>`` for plain dimensions and the source
    column for metrics. The source extracts the underlying warehouse column from
    either ``name`` (when it equals the column) or ``sql`` (when an alias is in
    play).
    """

    name: str
    table: str | None = None
    sql: str | None = None
    type: str | None = None
    label: str | None = None
    field_type: str | None = Field(default=None, alias="fieldType")


class LightdashExploreTable(_LightdashModel):
    """One entry of ``Explore.tables`` — the canonical place to read schema + sqlTable.

    ``sql_table`` is the raw, quoted warehouse path Lightdash compiles into SQL
    (e.g. ``\\`default\\`.\\`orders_enriched\\``). The source extracts the
    bare ``schema.table`` form for URN construction. The ``dimensions`` /
    ``metrics`` maps capture per-field metadata so the source can emit
    column-level chart lineage (``inputFields`` aspect on the Chart).
    """

    name: str
    schema_: str | None = Field(default=None, alias="schema")
    database: str | None = None
    sql_table: str | None = Field(default=None, alias="sqlTable")
    dimensions: dict[str, LightdashFieldDef] = Field(default_factory=dict)
    metrics: dict[str, LightdashFieldDef] = Field(default_factory=dict)


class LightdashExplore(_LightdashModel):
    """Full Explore as returned by ``GET /api/v1/projects/{uuid}/explores/{name}``.

    The ``tables`` map holds the base table plus any joined tables — keyed by
    table name. The source emits one upstream Dataset URN per entry, giving
    charts proper lineage even when the Explore joins multiple dbt models.
    """

    name: str
    label: str | None = None
    base_table: str | None = Field(default=None, alias="baseTable")
    target_database: str | None = Field(default=None, alias="targetDatabase")
    tables: dict[str, LightdashExploreTable] = Field(default_factory=dict)
    joined_tables: list[dict[str, Any]] = Field(default_factory=list, alias="joinedTables")
