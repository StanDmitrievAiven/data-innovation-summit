"""Source report for the Lightdash connector."""

from __future__ import annotations

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class LightdashSourceReport(StaleEntityRemovalSourceReport):
    """Extends DataHub's stale-entity report with Lightdash-specific counters.

    Numeric counters are surfaced verbatim in ``datahub ingest`` output, helping
    a demo presenter narrate "we emitted 13 charts and 2 dashboards" without
    grepping logs.
    """

    projects_scanned: int = 0
    projects_filtered: int = 0
    spaces_scanned: int = 0
    spaces_filtered: int = 0
    charts_scanned: int = 0
    charts_filtered: int = 0
    dashboards_scanned: int = 0
    dashboards_filtered: int = 0
    explores_resolved: int = 0
    explores_failed: list[str] = field(default_factory=list)
    warehouse_platform_fallbacks: list[str] = field(default_factory=list)

    def report_explore_failed(self, key: str) -> None:
        self.explores_failed.append(key)

    def report_warehouse_platform_fallback(self, warehouse_type: str) -> None:
        if warehouse_type not in self.warehouse_platform_fallbacks:
            self.warehouse_platform_fallbacks.append(warehouse_type)
