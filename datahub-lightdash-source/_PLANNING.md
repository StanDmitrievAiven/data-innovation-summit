# Lightdash DataHub Connector — Planning Document

**Created:** 2026-05-13
**Status:** IN_PROGRESS (awaiting approval)
**Author:** Stan Dmitriev (Data Innovation Summit demo)

---

## Overview

A DataHub source connector for [Lightdash](https://lightdash.com/) — a self-hosted BI tool used in this demo to render dashboards on top of ClickHouse views.

**Demo goal:** prove that Lightdash projects, spaces, dashboards, and saved charts can land in DataHub as first-class entities, with column-level lineage flowing through the existing `PostgreSQL → Kafka → ClickHouse` graph all the way up to Lightdash charts and dashboards.

**Deliverable:** a pip-installable Python package (`datahub-lightdash-source`) that registers a `source: type: lightdash` entry-point so it works both via:

- **Local push (now):** `datahub ingest -c lightdash.dhub.yaml` running on a laptop, with the DataHub REST sink pointing at the Aiven-hosted GMS. No Aiven changes required.
- **Aiven DataHub (later):** the same package added to *Ingestion → Settings → Extra Pip Libraries* (`datahub-lightdash-source==x.y.z`) plus a hosted recipe — matching the path the `datahub-airbyte-source` proved out.

---

## Research Summary

### Source Classification

| Field           | Value                                                                                                   |
| --------------- | ------------------------------------------------------------------------------------------------------- |
| Category        | **BI Tools**                                                                                            |
| Source type     | **api** (REST)                                                                                          |
| Interface       | Lightdash REST API (`/api/v1`)                                                                          |
| Auth            | Personal Access Token (PAT, `ldpat_…`) sent as `Authorization: ApiKey …` (verified against live API)    |
| Standards file  | `standards/source_types/bi_tools.md`                                                                    |
| API docs        | <https://docs.lightdash.com/references/api/>                                                            |
| Tested against  | Lightdash `0.2925.2`, instance at `https://019e0171-79d5-775d-b84a-9b12dcd1ed50-8080.eur-1.aiven.app`   |

### Lightdash domain model (relevant subset)

```
Organization (1)
└── Project (n)                          # e.g. "data-innovation-summit"
    ├── Warehouse connection             # ClickHouse @ clickhouse-2a6274d2 (PROD)
    ├── Explore (n)                      # dbt model exposed in Lightdash
    │   ├── baseTable                    # e.g. orders_enriched (ClickHouse view)
    │   ├── dimensions[] / metrics[]     # presentation layer
    │   └── joins[]
    └── Space (n)                        # "Shared", "My space", …
        ├── SavedChart (n)               # standalone chart, references one Explore
        └── Dashboard (n)
            └── tiles[]                  # references SavedChart
```

### Similar DataHub connectors

| Connector | Relevance | Why it’s useful |
| ----------- | ----------- | ----------------- |
| `looker`    | ⭐⭐⭐    | Same shape: project → folder → dashboard → look (=chart) → upstream warehouse table. Look at `looker_source.py` for chart/dashboard emission patterns. |
| `metabase`  | ⭐⭐⭐    | Closest stack-wise (REST API, single org, dashboards + saved questions on warehouses). |
| `superset`  | ⭐⭐    | REST API, dashboards + slices; useful for lineage-from-SQL parsing reference. |
| `mode`      | ⭐    | Smaller; useful for Pydantic response modelling style. |

We are not copying any of those connectors verbatim — they are all-in-tree connectors with much more behaviour (LookML compilation etc.). We use them as reference for entity shape and aspect choices only.

---

## Entity Mapping

| Lightdash concept     | DataHub entity   | Subtype       | URN                                                                            | Notes |
| --------------------- | ---------------- | ------------- | ------------------------------------------------------------------------------ | ----- |
| Organization          | Container        | `Organization`  | `urn:li:container:<sha1(lightdash:<org_uuid>)>`                              | Top-level, only emitted when `include_organization_container=True`. |
| Project               | Container        | `Project`     | `urn:li:container:<sha1(lightdash:<project_uuid>)>`                            | Parent: Organization (or none). |
| Space                 | Container        | `Folder`      | `urn:li:container:<sha1(lightdash:<space_uuid>)>`                              | Parent: Project. We do not model space nesting in v1 (Lightdash supports nested spaces, but tile-level URLs use the flat space). |
| Dashboard             | Dashboard        | `Dashboard`   | `urn:li:dashboard:(lightdash,<dashboard_uuid>)`                                | `dashboardInfo.charts` → list of chart URNs from `tiles[]`. |
| SavedChart            | Chart            | `Chart`       | `urn:li:chart:(lightdash,<chart_uuid>)`                                        | `inputs` = upstream warehouse dataset URNs derived from the Explore's baseTable + joins. |
| Explore (dbt model)   | *not emitted*    | —             | —                                                                              | We don't emit Explores as Datasets — the dbt model is the canonical Dataset (already in DataHub via the dbt source / ClickHouse source). We only use the Explore's `baseTable` + `joins` to compute upstream URNs for charts. |
| User                  | CorpUser         | —             | `urn:li:corpuser:<email_or_username>`                                          | Only emitted as the value of `ownership.owners[]` on Dashboard/Chart — we don't create CorpUser entities themselves (DataHub's identity is the source of truth). |

### Upstream URN resolution (lineage spine)

A Lightdash chart points at an Explore which has a `baseTable` and `joins` referencing dbt models. The connector resolves them to ClickHouse Dataset URNs:

```
chart.tableName             = "orders_enriched"
explore.baseTable           = "orders_enriched"
explore.joinedTables        = ["customers_clean", "products_catalog"]
warehouse_platform          = "clickhouse"
warehouse_database          = "default"             # from project config
platform_instance           = config.warehouse_platform_instance  (optional)
env                         = config.env  (default PROD)
```

→ `urn:li:dataset:(urn:li:dataPlatform:clickhouse,default.orders_enriched,PROD)`

These URNs must match what the existing ClickHouse ingestion source produces; the connector exposes the resolution as configurable so the URN format can be retargeted without code changes.

---

## Architecture Decisions

### Base class

`StatefulIngestionSourceBase` + `TestableSource` (the standard API-source combo, per `standards/api.md`).

### SDK V2 (mandatory for new connectors)

All entity emission goes through `datahub.sdk` classes:

| Entity     | SDK V2 class                                            |
| ---------- | ------------------------------------------------------- |
| Container  | `datahub.sdk.Container`                                 |
| Dashboard  | `datahub.sdk.Dashboard`                                 |
| Chart      | `datahub.sdk.Chart`                                     |
| Dataset    | Not emitted by us — referenced only by URN (see above). |

The `ExperimentalWarning` from `datahub.sdk.*` is acknowledged and suppressed at import time in the source module.

### File layout

```
datahub-lightdash-source/
├── pyproject.toml                              # entry-point registration
├── README.md                                   # install + recipe instructions
├── _PLANNING.md                                # this doc
├── src/datahub_lightdash_source/
│   ├── __init__.py
│   ├── client.py                               # LightdashClient (requests-based)
│   ├── config.py                               # LightdashSourceConfig + connection
│   ├── models.py                               # Pydantic response models
│   ├── report.py                               # LightdashSourceReport (extends StaleEntityRemovalSourceReport)
│   └── source.py                               # LightdashSource (entry point target)
├── tests/
│   └── unit/
│       ├── test_config.py
│       ├── test_client.py                      # responses-mocked HTTP
│       └── test_source.py                      # golden-style fixture test
└── recipes/
    └── lightdash.dhub.yaml                     # demo recipe → Aiven DataHub GMS
```

### Client design (`client.py`)

- `requests.Session` with `urllib3.Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])`.
- `Authorization: ApiKey <pat>` header from `SecretStr` (Lightdash-specific, verified against the live API; NOT `Bearer`).
- Methods used by the source:
  - `health()` → `GET /api/v1/health` (for `TestableSource.test_connection`).
  - `list_organizations()` → `GET /api/v1/org`.
  - `list_projects()` → `GET /api/v1/org/projects`.
  - `get_project(project_uuid)` → `GET /api/v1/projects/{uuid}` (warehouse type, dbt schema).
  - `list_spaces(project_uuid)` → `GET /api/v1/projects/{uuid}/spaces`.
  - `list_charts(project_uuid)` → `GET /api/v1/projects/{uuid}/charts`.
  - `get_chart(chart_uuid)` → `GET /api/v1/saved/{uuid}` (full chart config including `tableName`).
  - `list_dashboards(project_uuid)` → `GET /api/v1/projects/{uuid}/dashboards`.
  - `get_dashboard(dashboard_uuid)` → `GET /api/v1/dashboards/{uuid}` (tiles).
  - `list_explores(project_uuid)` → `GET /api/v1/projects/{uuid}/explores` (for join metadata).
  - `get_explore(project_uuid, explore_id)` → `GET /api/v1/projects/{uuid}/explores/{id}` (full join graph).

Lightdash responses are wrapped: `{"status": "ok", "results": ...}`. The client unwraps `results`.

Pagination: Lightdash endpoints return full lists (no cursor). We rely on the API's native behaviour and add a `page_size` config that's a no-op today but reserved for future paginated endpoints.

Rate limiting: not documented by Lightdash; we rely on retries.

### Config design (`config.py`)

```python
class LightdashConnectionConfig(ConfigModel):
    base_url: str                                          # https://lightdash.example.com
    personal_access_token: SecretStr                       # ldpat_…
    timeout_seconds: int = 30
    max_retries: int = 3
    verify_ssl: bool = True

class LightdashSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    connection: LightdashConnectionConfig

    # Scope
    project_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    space_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    dashboard_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    include_organization_container: bool = False

    # Feature toggles
    extract_owners: bool = True
    extract_lineage: bool = True

    # Warehouse URN resolution (for chart → dataset lineage)
    warehouse_platform: Optional[str] = None               # "clickhouse" / "snowflake" / "bigquery" — auto-detected from project.warehouse.type if None
    warehouse_platform_instance: Optional[str] = None
    warehouse_database_override: Optional[str] = None      # default: project.warehouse.schema
    warehouse_env: Optional[str] = None                    # default: top-level `env`
```

Validation:

- `connection.base_url` must start with `http://` or `https://`, trailing slash trimmed.
- If `warehouse_platform` is not set, the source auto-derives it from `project.warehouse.type` using the mapping in the next section.

### Warehouse type → DataHub platform mapping

| Lightdash `warehouse.type` | DataHub platform   |
| -------------------------- | ------------------ |
| `clickhouse`               | `clickhouse`       |
| `postgres`                 | `postgres`         |
| `redshift`                 | `redshift`         |
| `snowflake`                | `snowflake`        |
| `bigquery`                 | `bigquery`         |
| `databricks`               | `databricks`       |
| `trino`                    | `trino`            |
| `duckdb`                   | `duckdb`           |

Anything not in the table → fallback to `lightdash_warehouse` and a warning in the report; user can override via `warehouse_platform`.

---

## Capabilities

| `SourceCapability` | Priority   | Implementation notes |
| ------------------ | ---------- | --------------------- |
| `CONTAINERS`       | Required   | Org (optional) / Project / Space. |
| `DASHBOARDS`       | Required   | `dashboardInfo.charts` set from tiles. |
| `CHARTS`           | Required   | `chartInfo` with `chartUrl` and `inputs`. |
| `LINEAGE_COARSE`   | Required   | Chart → upstream warehouse Dataset URNs. Transitive Dashboard lineage is rendered by DataHub from chart inputs. |
| `OWNERSHIP`        | Optional   | From `dashboard.updatedByUser` and `chart.updatedByUser` if `extract_owners=True`. Owner type = `DATAOWNER`. |
| `PLATFORM_INSTANCE` | Optional  | Standard mixin. |
| `DELETION_DETECTION` | Optional | Via `StaleEntityRemovalHandler`. Required for stateful ingestion (re-runs clean up deleted dashboards/charts). |
| `TAGS`             | Out of scope (v1) | Lightdash has limited tagging (project-level labels only). Skip in v1. |
| `LINEAGE_FINE`     | Out of scope (v1) | Lightdash charts reference whole tables, not columns (dimensions are presentation-layer). |

---

## Known limitations

| Limitation | Impact | Workaround |
| ---------- | ------ | ---------- |
| Lightdash charts reference Explores at table level only; no column-level lineage from Lightdash itself. | Column lineage stops at the warehouse dataset. | Acceptable — the warehouse → dbt → ClickHouse lineage already carries columns. |
| Nested spaces are flattened in v1. | Visual hierarchy in DataHub doesn't mirror Lightdash's nested folders. | Defer to v2; rarely used in our demo. |
| Owner emails are not always exposed by `/api/v1/saved/{uuid}` (depends on Lightdash version). | Some charts won't have ownership. | Best-effort; report missing owners as warnings. |
| `warehouse.type` to platform mapping is static. | Less common warehouses fall back to a placeholder platform. | User can override via `warehouse_platform`. |
| Lightdash API doesn't expose chart "last viewed" / view counts. | No usage stats. | Out of scope (v1). |

---

## Testing Strategy

| Test type | What it covers | Location |
| --------- | --------------- | -------- |
| Unit — config | `LightdashSourceConfig` parsing, validators (`base_url`, warehouse platform autodetect). | `tests/unit/test_config.py` |
| Unit — client | HTTP behaviour with [`responses`](https://github.com/getsentry/responses): bearer header, error mapping, JSON unwrap. | `tests/unit/test_client.py` |
| Unit — source | Source emits the expected SDK V2 entities for a synthetic Lightdash project (1 space, 1 dashboard, 2 charts, 2 explores). Snapshot test asserts URN set + key aspects. | `tests/unit/test_source.py` |
| Integration | Not in v1 — Lightdash needs a full Docker stack (Postgres state + warehouse). Manual smoke test against the live Aiven Lightdash. | — |

Coverage target: ≥80% on the package.

---

## Demo recipe (push approach)

`recipes/lightdash.dhub.yaml`:

```yaml
source:
  type: lightdash
  config:
    env: PROD
    connection:
      base_url: ${LIGHTDASH_URL}
      personal_access_token: ${LIGHTDASH_PAT}
    warehouse_platform: clickhouse
    warehouse_database_override: default

sink:
  type: datahub-rest
  config:
    server: ${DATAHUB_GMS_URL}
    token: ${DATAHUB_GMS_TOKEN}
```

Invocation (local, push to Aiven GMS):

```bash
pip install -e /Users/stan.dmitriev/Documents/DataInnovationSummitDemo/datahub-lightdash-source
export LIGHTDASH_URL='https://019e0171-…-eur-1.aiven.app'
export LIGHTDASH_PAT='ldpat_…'
export DATAHUB_GMS_URL='https://019db50f-…-eur-1.aiven.app'
export DATAHUB_GMS_TOKEN='eyJhbGciOi…'

datahub ingest -c recipes/lightdash.dhub.yaml
```

---

## Implementation Order

1. **Skeleton + entry point**
   - `pyproject.toml` declares `datahub.ingestion.source.plugins.lightdash = datahub_lightdash_source.source:LightdashSource`.
   - Minimal `LightdashSource` returns zero workunits; verify `datahub ingest` boots and prints a clean report.
2. **`client.py`** — `LightdashClient` with health/projects/spaces/charts/dashboards/explores.
3. **`models.py`** — Pydantic v2 response models for the subset of fields we read.
4. **`config.py`** — `LightdashConnectionConfig` and `LightdashSourceConfig` with validators.
5. **`source.py` — Containers**
   - Emit Project + Space containers; verify they appear in DataHub.
6. **`source.py` — Charts**
   - Emit Chart entities with `chartUrl`, `external_url`, `chart_type`, owners.
   - Resolve `inputs` (warehouse Dataset URNs from Explore's `baseTable` + `joinedTables`).
7. **`source.py` — Dashboards**
   - Emit Dashboard entities with `charts` (list of chart URNs from tiles) and `dashboardUrl`.
8. **`source.py` — Ownership** — from `updatedByUser`.
9. **`source.py` — Stale-entity removal** — wire `StaleEntityRemovalHandler` for delete tracking.
10. **Tests** — config, client, source.
11. **`README.md`** — install, recipe, "load as Extra Pip Libraries" note.
12. **`recipes/lightdash.dhub.yaml`** — finalised; run end-to-end against Aiven Lightdash and Aiven DataHub.

---

## Approval

- [ ] User approved this plan on: ____
- [ ] Approval message: ____
