# DataHub Lightdash source

A [DataHub](https://datahubproject.io) ingestion source for
[Lightdash](https://www.lightdash.com/) — extracts Projects, Spaces,
Dashboards, and Saved Charts, with chart-level lineage pointing at the
underlying warehouse datasets.

Built and validated against:

- DataHub `acryl-datahub` ≥ 0.14
- Lightdash 0.2925.x (self-hosted)
- Aiven for Apache Lightdash + Aiven for ClickHouse (the original use case)

## What it produces

| Lightdash concept | DataHub entity | Subtype       |
| ----------------- | -------------- | ------------- |
| Organization      | `Container`    | `Organization` (opt-in) |
| Project           | `Container`    | `Project`     |
| Space             | `Container`    | `Folder`      |
| Dashboard         | `Dashboard`    | (default)     |
| Saved chart       | `Chart`        | (default)     |

Every chart also emits a `chartInfo.inputs` edge per table in the chart's
Lightdash *Explore* — base table + joined tables — giving you proper
warehouse lineage (ClickHouse, Snowflake, BigQuery, Postgres, …). The
warehouse platform is auto-detected from `project.warehouseConnection.type`,
and you can override it via the `warehouse_platform` config when your
DataHub source uses a non-default platform name.

## Install

```bash
pip install -e /path/to/datahub-lightdash-source
```

> The package registers itself under the standard
> `datahub.ingestion.source.plugins` entry-point group as `lightdash`, so
> after install you can run `datahub check plugins | grep lightdash` to
> confirm registration.

If you're running ingestion on a managed DataHub (e.g. Aiven for DataHub),
the simplest path is to push this package to a Git repo and reference it
under **Extra Pip Libraries**, e.g.:

```
git+https://github.com/StanDmitrievAiven/datahub-lightdash-source.git@main
```

## Configure

Create a Personal Access Token in Lightdash (Settings → Personal access
tokens). Then point a DataHub recipe at it:

```yaml
source:
  type: lightdash
  config:
    connection:
      base_url: https://lightdash.example.com
      personal_access_token: ${LIGHTDASH_PAT}

    # Optional — match the platform name your existing DataHub warehouse
    # source uses for the same warehouse.
    warehouse_platform: clickhouse
    # If your ClickHouse/Snowflake source uses platform_instance, mirror it
    # here so chart upstreams join onto the right Dataset URNs.
    # warehouse_platform_instance: prod-eu

    # Optional — defaults to PROD.
    env: PROD

    # Optional regex allow/deny.
    project_pattern:
      allow: ["^Aiven$"]
    space_pattern:
      allow: [".*"]
    dashboard_pattern:
      allow: [".*"]
    chart_pattern:
      allow: [".*"]

    # Defaults: True.
    extract_owners: true
    extract_lineage: true

    # Defaults to False — most demos don't need a top-level Organization
    # container above the projects.
    include_organization_container: false

    # Recommended in production — track and soft-delete entities that
    # disappear from Lightdash between runs.
    stateful_ingestion:
      enabled: true
      remove_stale_metadata: true

sink:
  type: datahub-rest
  config:
    server: ${DATAHUB_GMS_URL}
    token: ${DATAHUB_TOKEN}

pipeline_name: lightdash-ingest
```

## Test connection from DataHub UI

The source registers itself as a *Testable Source*, so the **Test
connection** button on the DataHub managed-ingestion UI runs:

1. An unauthenticated `GET /api/v1/health` — confirms the URL points at a
   live Lightdash instance.
2. An authenticated `GET /api/v1/org` — confirms the PAT is valid and
   uses the correct `Authorization: ApiKey <pat>` header.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

pytest                  # unit tests
ruff check src tests    # lint
mypy src                # types
```

The test suite is fully offline — it mocks `LightdashClient`, no live
Lightdash instance required.

## Known limitations (v0.1)

- **Charts are tracked by UUID**, not slug. If you delete and recreate a
  chart with the same slug, the new one becomes a fresh entity.
- **Tile-level dimension/measure lineage is coarse only**. The `chartInfo`
  aspect ships dataset-level edges; column-level lineage from
  Lightdash dimensions back to warehouse columns is out of scope for v0.1.
- **CorpUser URNs prefer email**, falling back to the Lightdash
  `userUuid`. Older Lightdash builds don't expose user emails on the
  `updatedByUser` payload — those owners use the UUID form. For best
  results, also ingest your IdP (Okta/Azure-AD) into DataHub so the UUID
  variant survives the duplicate-detection step.

## License

Apache 2.0
