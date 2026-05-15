"""DataHub ingestion source for Lightdash.

Wires :class:`LightdashClient` into DataHub's stateful ingestion pipeline and
emits SDK V2 entities for Lightdash projects, spaces, dashboards, and saved
charts — with chart-level lineage pointing at the underlying warehouse
datasets (typically ClickHouse, but resolved from each Explore's compiled
table reference).
"""

from __future__ import annotations

import logging
import re
import warnings
from collections.abc import Iterable

# The new SDK is marked experimental upstream; suppress the warning so demos
# don't get a Sentry-style red blob on stderr for every entity we emit.
from datahub.errors import ExperimentalWarning

warnings.filterwarnings("ignore", category=ExperimentalWarning)

from datahub.emitter.mce_builder import make_schema_field_urn  # noqa: E402
from datahub.emitter.mcp import MetadataChangeProposalWrapper  # noqa: E402
from datahub.emitter.mcp_builder import ContainerKey  # noqa: E402
from datahub.ingestion.api.common import PipelineContext  # noqa: E402
from datahub.ingestion.api.decorators import (  # noqa: E402
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (  # noqa: E402
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit  # noqa: E402
from datahub.ingestion.source.state.stale_entity_removal_handler import (  # noqa: E402
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (  # noqa: E402
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (  # noqa: E402
    EdgeClass,
    InputFieldClass,
    InputFieldsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)
from datahub.metadata.urns import ChartUrn, DatasetUrn  # noqa: E402
from datahub.sdk import Chart, Container, Dashboard  # noqa: E402

from datahub_lightdash_source.client import (  # noqa: E402
    LightdashAPIError,
    LightdashClient,
)
from datahub_lightdash_source.config import (  # noqa: E402
    PLATFORM_NAME,
    LightdashSourceConfig,
)
from datahub_lightdash_source.models import (  # noqa: E402
    LightdashChart,
    LightdashChartSummary,
    LightdashExplore,
    LightdashProject,
    LightdashProjectSummary,
    LightdashSpace,
    LightdashUserRef,
)
from datahub_lightdash_source.report import LightdashSourceReport  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Container key hierarchy
# ---------------------------------------------------------------------------
# These subclasses form an explicit Organization → Project → Space chain.
# ``ContainerKey.parent_key()`` walks ``__bases__``, so each subclass returns
# the immediate parent automatically — that's how SDK V2 attaches the
# correct ``container`` aspect without us touching it manually.


class LightdashOrgKey(ContainerKey):
    organization_uuid: str


class LightdashProjectKey(LightdashOrgKey):
    project_uuid: str


class LightdashSpaceKey(LightdashProjectKey):
    space_uuid: str


# Lightdash chartKind → DataHub ChartTypeClass enum value. Only DataHub-defined
# values (BAR, PIE, SCATTER, TABLE, TEXT, LINE, AREA, HISTOGRAM, BOX_PLOT,
# WORD_CLOUD, COHORT) are allowed; anything else is dropped and the chart is
# emitted without a ``chartType`` (still searchable, just unstyled).
CHART_KIND_TO_DATAHUB: dict[str, str] = {
    "table": "TABLE",
    "big_number": "TEXT",
    "line": "LINE",
    "area": "AREA",
    "scatter": "SCATTER",
    "pie": "PIE",
    "vertical_bar": "BAR",
    "horizontal_bar": "BAR",
    "cartesian": "BAR",
    "mixed": "BAR",
    "funnel": "BAR",
    "treemap": "TABLE",
}


@platform_name("Lightdash")
@config_class(LightdashSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Descriptions on Dashboards, Charts, Containers")
@capability(SourceCapability.CONTAINERS, "Project + Space hierarchy")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Chart → warehouse dataset lineage from Explore tables (ClickHouse/Snowflake/etc.)",
)
@capability(SourceCapability.OWNERSHIP, "Owners taken from updatedByUser", supported=True)
@capability(SourceCapability.PLATFORM_INSTANCE, "Multi-instance Lightdash support")
@capability(SourceCapability.TEST_CONNECTION, "Health-check + authenticated org probe")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Soft-delete entities removed from Lightdash between runs (stateful ingestion)",
    supported=True,
)
class LightdashSource(StatefulIngestionSourceBase, TestableSource):
    """Ingest Lightdash metadata into DataHub.

    Order of emission inside :meth:`get_workunits_internal`:

    1. Organization container (optional, off by default).
    2. For each project that matches ``project_pattern``:
       - Project container, then Space containers under it.
       - All saved charts in the project, with upstream warehouse Dataset URNs.
       - All dashboards in the project, referencing the chart URNs from above.

    Charts are emitted before dashboards so dashboards can reliably reference
    them by URN. The deletion handler runs after via
    :class:`StaleEntityRemovalHandler`'s workunit processor.
    """

    def __init__(self, config: LightdashSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = LightdashSourceReport()
        self.client = LightdashClient(
            base_url=config.connection.base_url,
            personal_access_token=config.connection.personal_access_token.get_secret_value(),
            timeout_seconds=config.connection.timeout_seconds,
            max_retries=config.connection.max_retries,
            verify_ssl=config.connection.verify_ssl,
        )
        # Cache: avoid re-fetching the same Explore for every chart that uses it.
        self._explore_cache: dict[tuple[str, str], LightdashExplore | None] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> LightdashSource:
        config = LightdashSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    # ---- TestableSource ----------------------------------------------------

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = LightdashSourceConfig.model_validate(config_dict)
            client = LightdashClient(
                base_url=config.connection.base_url,
                personal_access_token=config.connection.personal_access_token.get_secret_value(),
                timeout_seconds=config.connection.timeout_seconds,
                max_retries=config.connection.max_retries,
                verify_ssl=config.connection.verify_ssl,
            )
            try:
                health = client.health()
                if not health.get("healthy", False):
                    return TestConnectionReport(
                        basic_connectivity=CapabilityReport(
                            capable=False,
                            failure_reason=f"Lightdash reports unhealthy: {health}",
                        ),
                    )
                # Round-trip an authenticated call too so we catch bad PATs.
                client.get_organization()
            finally:
                client.close()
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True),
            )
        except Exception as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=False, failure_reason=str(e)),
            )

    # ---- Stateful ingestion ------------------------------------------------

    def get_workunit_processors(self) -> list[MetadataWorkUnitProcessor | None]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> LightdashSourceReport:
        return self.report

    def close(self) -> None:
        self.client.close()
        super().close()

    # ---- Main pipeline -----------------------------------------------------

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        org = self.client.get_organization()

        if self.config.include_organization_container:
            yield from self._emit_organization(org.organization_uuid, org.name)

        for project_summary in self.client.list_projects():
            self.report.projects_scanned += 1
            if not self.config.project_pattern.allowed(project_summary.name):
                self.report.projects_filtered += 1
                continue

            yield from self._process_project(org.organization_uuid, project_summary)

    # ---- Organization ------------------------------------------------------

    def _org_key(self, organization_uuid: str) -> LightdashOrgKey:
        return LightdashOrgKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            env=self.config.env,
            organization_uuid=organization_uuid,
        )

    def _emit_organization(
        self, organization_uuid: str, name: str
    ) -> Iterable[MetadataWorkUnit]:
        key = self._org_key(organization_uuid)
        container = Container(
            container_key=key,
            display_name=name,
            subtype="Organization",
            description=f"Lightdash organization {organization_uuid}",
            extra_properties={"lightdashOrganizationUuid": organization_uuid},
            parent_container=None,
        )
        yield from container.as_workunits()

    # ---- Project -----------------------------------------------------------

    def _project_key(self, project: LightdashProjectSummary | LightdashProject) -> LightdashProjectKey:
        return LightdashProjectKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            env=self.config.env,
            organization_uuid=project.organization_uuid if isinstance(project, LightdashProject) else "",
            project_uuid=project.project_uuid,
        )

    def _process_project(
        self,
        organization_uuid: str,
        project_summary: LightdashProjectSummary,
    ) -> Iterable[MetadataWorkUnit]:
        try:
            project = self.client.get_project(project_summary.project_uuid)
        except LightdashAPIError as e:
            self.report.warning(
                "project_fetch_failed",
                f"Could not fetch project {project_summary.project_uuid}: {e}",
            )
            return

        # Resolve which DataHub platform the project's warehouse maps to. This
        # determines the URN format of every chart's upstream Dataset edge.
        warehouse_type = (
            project.warehouse_connection.type if project.warehouse_connection else None
        )
        warehouse_platform, auto_detected = self.config.resolve_warehouse_platform(
            warehouse_type
        )
        if (
            auto_detected
            and warehouse_type
            and warehouse_platform == PLATFORM_NAME
        ):
            self.report.report_warehouse_platform_fallback(warehouse_type)
            self.report.warning(
                "warehouse_platform_fallback",
                f"Unrecognised Lightdash warehouseType {warehouse_type!r}; chart "
                f"upstreams will use '{PLATFORM_NAME}' as the data-platform. "
                f"Override with warehouse_platform in the source config.",
            )

        # Project container
        project_key = LightdashProjectKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            env=self.config.env,
            organization_uuid=organization_uuid,
            project_uuid=project.project_uuid,
        )
        project_external_url = f"{self.config.connection.base_url}/projects/{project.project_uuid}"
        project_props = {
            "lightdashProjectUuid": project.project_uuid,
            "warehouseType": warehouse_type or "",
            "projectType": project.type or "",
        }
        project_container = Container(
            container_key=project_key,
            display_name=project.name,
            subtype="Project",
            external_url=project_external_url,
            extra_properties=project_props,
            parent_container=(
                self._org_key(organization_uuid)
                if self.config.include_organization_container
                else None
            ),
        )
        yield from project_container.as_workunits()

        # Spaces -> in-memory index so chart/dashboard emission can resolve them
        spaces_by_uuid: dict[str, LightdashSpace] = {}
        for space in self.client.list_spaces(project.project_uuid):
            self.report.spaces_scanned += 1
            if not self.config.space_pattern.allowed(space.name):
                self.report.spaces_filtered += 1
                continue
            spaces_by_uuid[space.uuid] = space
            yield from self._emit_space(project_key, project.project_uuid, space)

        # Charts (must come before dashboards so chart URNs are emitted first)
        for chart_summary in self.client.list_charts(project.project_uuid):
            self.report.charts_scanned += 1
            if not self.config.chart_pattern.allowed(chart_summary.name):
                self.report.charts_filtered += 1
                continue
            if (
                chart_summary.space_uuid
                and chart_summary.space_uuid not in spaces_by_uuid
            ):
                # Chart lives in a space filtered out above — skip it for consistency.
                self.report.charts_filtered += 1
                continue
            yield from self._process_chart(
                project=project,
                chart_summary=chart_summary,
                warehouse_platform=warehouse_platform,
                spaces_by_uuid=spaces_by_uuid,
            )

        # Dashboards
        for dashboard_summary in self.client.list_dashboards(project.project_uuid):
            self.report.dashboards_scanned += 1
            if not self.config.dashboard_pattern.allowed(dashboard_summary.name):
                self.report.dashboards_filtered += 1
                continue
            if (
                dashboard_summary.space_uuid
                and dashboard_summary.space_uuid not in spaces_by_uuid
            ):
                self.report.dashboards_filtered += 1
                continue
            yield from self._process_dashboard(
                project_uuid=project.project_uuid,
                dashboard_uuid=dashboard_summary.uuid,
                spaces_by_uuid=spaces_by_uuid,
            )

    # ---- Space -------------------------------------------------------------

    def _space_key(
        self,
        project_key_fields: tuple[str, str],
        space_uuid: str,
    ) -> LightdashSpaceKey:
        organization_uuid, project_uuid = project_key_fields
        return LightdashSpaceKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            env=self.config.env,
            organization_uuid=organization_uuid,
            project_uuid=project_uuid,
            space_uuid=space_uuid,
        )

    def _emit_space(
        self,
        project_key: LightdashProjectKey,
        project_uuid: str,
        space: LightdashSpace,
    ) -> Iterable[MetadataWorkUnit]:
        space_key = LightdashSpaceKey(
            platform=PLATFORM_NAME,
            instance=self.config.platform_instance,
            env=self.config.env,
            organization_uuid=project_key.organization_uuid,
            project_uuid=project_uuid,
            space_uuid=space.uuid,
        )
        space_external_url = (
            f"{self.config.connection.base_url}/projects/{project_uuid}/spaces/{space.uuid}"
        )
        container = Container(
            container_key=space_key,
            display_name=space.name,
            subtype="Folder",
            external_url=space_external_url,
            extra_properties={
                "lightdashSpaceUuid": space.uuid,
                "lightdashSpaceSlug": space.slug or "",
                "lightdashChartCount": str(space.chart_count or ""),
                "lightdashDashboardCount": str(space.dashboard_count or ""),
            },
        )
        yield from container.as_workunits()

    # ---- Charts ------------------------------------------------------------

    def _get_explore(self, project_uuid: str, explore_name: str) -> LightdashExplore | None:
        cache_key = (project_uuid, explore_name)
        if cache_key in self._explore_cache:
            return self._explore_cache[cache_key]
        try:
            explore = self.client.get_explore(project_uuid, explore_name)
            self.report.explores_resolved += 1
        except LightdashAPIError as e:
            self.report.report_explore_failed(f"{project_uuid}/{explore_name}")
            self.report.warning(
                "explore_fetch_failed",
                f"Could not fetch explore {explore_name} in project {project_uuid}: {e}",
            )
            explore = None
        self._explore_cache[cache_key] = explore
        return explore

    def _upstream_dataset_urns(
        self,
        chart: LightdashChart,
        warehouse_platform: str,
    ) -> list[str]:
        """Resolve a chart's upstream warehouse Dataset URNs.

        The chart's ``metric_query.explore_name`` (falling back to ``table_name``)
        identifies its Explore. We then fetch the Explore detail and emit one
        upstream URN per entry in ``tables`` — that covers the base table plus
        any joined dbt models.
        """
        if not self.config.extract_lineage:
            return []

        explore_name = (
            chart.metric_query.explore_name if chart.metric_query else None
        ) or chart.table_name
        if not explore_name:
            return []

        explore = self._get_explore(chart.project_uuid, explore_name)
        if explore is None or not explore.tables:
            # Fall back to the chart's own ``table_name`` so charts still get
            # *some* upstream URN even if explore fetch fails.
            if chart.table_name:
                return [
                    self._make_dataset_urn(
                        warehouse_platform=warehouse_platform,
                        schema=self.config.warehouse_database_override or "default",
                        table=chart.table_name,
                    )
                ]
            return []

        urns: list[str] = []
        for table_name, table in explore.tables.items():
            schema = (
                self.config.warehouse_database_override
                or table.schema_
                or "default"
            )
            urns.append(
                self._make_dataset_urn(
                    warehouse_platform=warehouse_platform,
                    schema=schema,
                    table=table_name,
                )
            )
        return urns

    def _make_dataset_urn(
        self,
        *,
        warehouse_platform: str,
        schema: str,
        table: str,
    ) -> str:
        env = self.config.warehouse_env or self.config.env or "PROD"
        return str(
            DatasetUrn.create_from_ids(
                platform_id=warehouse_platform,
                table_name=f"{schema}.{table}",
                env=env,
                platform_instance=self.config.warehouse_platform_instance,
            )
        )

    # ---- Column-level chart lineage ---------------------------------------

    # Lightdash compiles SQL via ``${TABLE}.<column>`` placeholders for both
    # dimensions and metrics. For column-level chart lineage we just need the
    # bare column name, which this regex extracts. ``LOWER(${TABLE}.created_at)``
    # → ``created_at``; ``${TABLE}.total`` → ``total``.
    _TABLE_COL_RE = re.compile(r"\$\{\s*TABLE\s*\}\s*\.\s*([\"`]?)(\w+)\1")

    @classmethod
    def _columns_from_lightdash_sql(cls, sql: str | None) -> list[str]:
        if not sql:
            return []
        return [m.group(2) for m in cls._TABLE_COL_RE.finditer(sql)]

    def _chart_input_fields(
        self,
        chart: LightdashChart,
        warehouse_platform: str,
    ) -> list[InputFieldClass]:
        """Resolve each chart dimension/metric to a ``schemaFieldUrn`` on the
        upstream warehouse table.

        Algorithm:
        1. The Lightdash field IDs in ``metricQuery.dimensions`` /
           ``metricQuery.metrics`` are of the shape ``<table>_<fieldName>``.
        2. Strip the matching table prefix (one of the Explore's table names).
        3. Look up the resulting field in ``explore.tables[<table>].dimensions``
           or ``.metrics`` to find the underlying warehouse column. Dimensions
           usually expose the column as ``LightdashFieldDef.name``; metrics
           expose it inside ``sql`` (``${TABLE}.<column>``).
        """
        if not self.config.extract_lineage or chart.metric_query is None:
            return []

        explore_name = chart.metric_query.explore_name or chart.table_name
        if not explore_name:
            return []
        explore = self._get_explore(chart.project_uuid, explore_name)
        if explore is None or not explore.tables:
            return []

        # Order: longest table name first so e.g. ``orders_enriched_order_id``
        # matches the ``orders_enriched`` prefix before ``orders``.
        table_names = sorted(explore.tables.keys(), key=len, reverse=True)

        input_fields: list[InputFieldClass] = []
        seen: set[str] = set()

        all_field_ids = list(chart.metric_query.dimensions or []) + list(
            chart.metric_query.metrics or []
        )

        for field_id in all_field_ids:
            table_name = next(
                (t for t in table_names if field_id.startswith(f"{t}_")), None
            )
            if table_name is None:
                continue
            local_field_name = field_id[len(table_name) + 1 :]

            table = explore.tables[table_name]
            field_def = (table.dimensions or {}).get(local_field_name) or (
                table.metrics or {}
            ).get(local_field_name)

            if field_def is None:
                continue

            # Dimensions: ``name`` is typically the column name directly.
            # Metrics: name is the metric identifier (e.g. ``total_revenue``),
            # but ``sql`` references the underlying column(s). Extract from sql
            # if available; fall back to the field name for dimensions.
            columns: list[str] = []
            if field_def.sql:
                columns = self._columns_from_lightdash_sql(field_def.sql)
            if not columns:
                # No ${TABLE} reference (e.g. ``count(*)`` style metric) — use the
                # field's own ``name`` for dimensions; skip metrics that don't
                # cleanly reference a column.
                in_dimensions = local_field_name in (table.dimensions or {})
                if in_dimensions:
                    columns = [field_def.name]
                else:
                    continue

            schema = (
                self.config.warehouse_database_override
                or table.schema_
                or "default"
            )
            dataset_urn = self._make_dataset_urn(
                warehouse_platform=warehouse_platform,
                schema=schema,
                table=table_name,
            )
            for col in columns:
                schema_field_urn = make_schema_field_urn(dataset_urn, col)
                if schema_field_urn in seen:
                    continue
                seen.add(schema_field_urn)
                # DataHub's pipeline filters out input fields where the embedded
                # ``schemaField.fieldPath`` is missing — see "Invalid input
                # fields filtered" in the ingestion report. Populate a minimal
                # SchemaField alongside the URN so the aspect survives.
                input_fields.append(
                    InputFieldClass(
                        schemaFieldUrn=schema_field_urn,
                        schemaField=SchemaFieldClass(
                            fieldPath=col,
                            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                            nativeDataType="",
                        ),
                    )
                )

        return input_fields

    def _process_chart(
        self,
        project: LightdashProject,
        chart_summary: LightdashChartSummary,
        warehouse_platform: str,
        spaces_by_uuid: dict[str, LightdashSpace],
    ) -> Iterable[MetadataWorkUnit]:
        try:
            chart = self.client.get_chart(chart_summary.uuid)
        except LightdashAPIError as e:
            self.report.warning(
                "chart_fetch_failed",
                f"Could not fetch chart {chart_summary.uuid}: {e}",
            )
            return

        chart_url = (
            f"{self.config.connection.base_url}"
            f"/projects/{chart.project_uuid}/saved/{chart.uuid}/view"
        )
        # The saved-chart detail endpoint does NOT include chart_kind/chart_type
        # at the top level (they live under chartConfig.type). The list endpoint
        # does — so we carry them through on the summary object.
        chart_kind = (
            chart_summary.chart_kind or chart_summary.chart_type
        ) or (chart.chart_kind or chart.chart_type)
        chart_type = CHART_KIND_TO_DATAHUB.get(chart_kind or "")

        parent_container = None
        if chart.space_uuid and chart.space_uuid in spaces_by_uuid:
            parent_container = self._space_key(
                (project.organization_uuid, project.project_uuid),
                chart.space_uuid,
            )

        owners = (
            self._owners_from_user(chart.updated_by_user)
            if self.config.extract_owners
            else None
        )

        custom_props: dict[str, str] = {
            "lightdashChartUuid": chart.uuid,
            "lightdashChartKind": chart_kind or "",
            "lightdashChartType": (
                chart_summary.chart_type or chart.chart_type or ""
            ),
            "lightdashTableName": chart.table_name or "",
            "lightdashSpaceName": chart.space_name or chart_summary.space_name or "",
            "lightdashSlug": chart.slug or chart_summary.slug or "",
            "lightdashProjectUuid": chart.project_uuid,
        }

        upstream_urns = self._upstream_dataset_urns(chart, warehouse_platform)
        sdk_chart = Chart(
            platform=PLATFORM_NAME,
            platform_instance=self.config.platform_instance,
            name=chart.uuid,
            display_name=chart.name,
            description=chart.description or "",
            chart_url=chart_url,
            external_url=chart_url,
            custom_properties=custom_props,
            last_modified=chart.updated_at,
            chart_type=chart_type,
            input_datasets=upstream_urns,
            parent_container=parent_container,
            owners=owners,
        )
        # SDK V2 only sets the legacy ``chartInfo.inputs`` field. DataHub's graph
        # indexer reads lineage from ``inputEdges`` (the Edge[] form) — without
        # it, the chart-→-dataset edge never makes it into the graph. We mirror
        # the inputs into ``inputEdges`` before the workunits leave the source.
        for wu in sdk_chart.as_workunits():
            if (
                wu.metadata.aspectName == "chartInfo"
                and upstream_urns
                and getattr(wu.metadata.aspect, "inputEdges", None) is None
            ):
                wu.metadata.aspect.inputEdges = [
                    EdgeClass(destinationUrn=urn) for urn in upstream_urns
                ]
            yield wu

        # Column-level chart lineage: emit an ``inputFields`` aspect mapping
        # each Lightdash dimension/metric back to its warehouse column. This
        # populates the "Fields" column on the chart's lineage page and lets
        # DataHub's column-impact view trace which warehouse columns feed
        # which Lightdash dimensions.
        chart_urn = str(
            ChartUrn.create_from_ids(
                platform=PLATFORM_NAME,
                name=chart.uuid,
                platform_instance=self.config.platform_instance,
            )
        )
        input_fields = self._chart_input_fields(chart, warehouse_platform)
        if input_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(fields=input_fields),
            ).as_workunit()

    # ---- Dashboards --------------------------------------------------------

    def _process_dashboard(
        self,
        project_uuid: str,
        dashboard_uuid: str,
        spaces_by_uuid: dict[str, LightdashSpace],
    ) -> Iterable[MetadataWorkUnit]:
        try:
            dashboard = self.client.get_dashboard(dashboard_uuid)
        except LightdashAPIError as e:
            self.report.warning(
                "dashboard_fetch_failed",
                f"Could not fetch dashboard {dashboard_uuid}: {e}",
            )
            return

        dashboard_url = (
            f"{self.config.connection.base_url}"
            f"/projects/{project_uuid}/dashboards/{dashboard.uuid}/view"
        )

        chart_urn_objects: list[str] = []
        for tile in dashboard.tiles:
            if tile.type != "saved_chart" or tile.properties is None:
                continue
            if not tile.properties.saved_chart_uuid:
                continue
            chart_urn_objects.append(
                str(
                    ChartUrn.create_from_ids(
                        platform=PLATFORM_NAME,
                        name=tile.properties.saved_chart_uuid,
                        platform_instance=self.config.platform_instance,
                    )
                )
            )

        parent_container = None
        if dashboard.space_uuid and dashboard.space_uuid in spaces_by_uuid:
            parent_container = self._space_key(
                ("", project_uuid),
                dashboard.space_uuid,
            )

        owners = (
            self._owners_from_user(dashboard.updated_by_user)
            if self.config.extract_owners
            else None
        )

        sdk_dashboard = Dashboard(
            platform=PLATFORM_NAME,
            platform_instance=self.config.platform_instance,
            name=dashboard.uuid,
            display_name=dashboard.name,
            description=dashboard.description or "",
            dashboard_url=dashboard_url,
            external_url=dashboard_url,
            custom_properties={
                "lightdashDashboardUuid": dashboard.uuid,
                "lightdashProjectUuid": project_uuid,
                "tileCount": str(len(dashboard.tiles)),
            },
            last_modified=dashboard.updated_at,
            charts=chart_urn_objects,
            parent_container=parent_container,
            owners=owners,
        )
        yield from sdk_dashboard.as_workunits()

    # ---- Ownership ---------------------------------------------------------

    @staticmethod
    def _owners_from_user(user: LightdashUserRef | None) -> list[str] | None:
        if user is None:
            return None
        # Prefer the email URN form (matches DataHub's normal CorpUser ingestion),
        # falling back to the Lightdash userUuid otherwise.
        identifier = user.email or user.user_uuid
        return [f"urn:li:corpuser:{identifier}"]
