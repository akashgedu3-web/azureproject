# Spotify Medallion Architecture — Azure Data Factory + Databricks Unity Catalog

An end-to-end Azure data engineering pipeline that ingests Spotify-style listening data through a full **Bronze → Silver → Gold medallion architecture**, using Azure Data Factory for orchestration and incremental CDC ingestion, and Databricks (Unity Catalog + Lakeflow Declarative Pipelines) for transformation, streaming, and slowly changing dimensions.

> This folder covers the **Databricks** side of the project. The Azure Data Factory pipelines that feed it live in the parent repo (`/dataset`, `/factory`, `/pipeline`, `/linkedService`).

---

## Architecture

```
Azure SQL DB (source)
        │  CDC + watermark incremental load
        ▼
   Azure Data Factory  (df-azureproject03)
        │  writes raw files
        ▼
  ADLS Gen2 — Bronze layer  (storageazureproject03/bronze)
        │  Databricks Auto Loader (cloudFiles), streaming
        ▼
  Delta Lake — Silver layer  (spotify_cata.silver.*)
        │  Lakeflow Declarative Pipelines (DLT), streaming + Auto CDC (SCD Type 2)
        ▼
  Delta Lake — Gold layer  (spotify_cata.gold.*)
        │  star schema: dimensions + fact table
        ▼
   Analytics-ready tables in Unity Catalog
```

**Governance layer:** all Bronze/Silver/Gold data is registered in a Unity Catalog metastore (`metastore_centralus03`), with storage access managed through an Azure Access Connector and a dedicated storage credential — not workspace-level service principals.

---

## Tech Stack

| Layer | Tools |
|---|---|
| Orchestration & ingestion | Azure Data Factory, CDC + watermark-based incremental pipelines |
| Storage | Azure Data Lake Storage Gen2 (containers: `bronze`, `silver`, `gold`, `cdc`) |
| Source database | Azure SQL Database |
| Processing | Azure Databricks, PySpark, Spark Structured Streaming (Auto Loader) |
| Transformation framework | Lakeflow Declarative Pipelines (DLT) — streaming tables + Auto CDC flows |
| Governance | Unity Catalog, Access Connector, Storage Credentials, External Locations |
| Deployment / IaC | Databricks Asset Bundles (DABs) — YAML-defined pipelines, CLI-based deploy, dev/prod targets |
| Data modeling | Star schema, Slowly Changing Dimensions (SCD Type 2) |

---

## What's in the Gold Layer

Built as a single Lakeflow Declarative Pipeline (`gold_pipeline`), deployed via a Databricks Asset Bundle rather than the workspace UI.

| Table | Type | Description | Rows (sample run) |
|---|---|---|---|
| `dim_user` | Dimension, SCD Type 2 | User profile history with full change tracking | 500 |
| `dim_track` | Dimension, SCD Type 2 | Track metadata history | 502 |
| `dim_date` | Dimension | Calendar dimension | 365 |
| `factstream` | Fact | Streaming/listening events, joined against dimension keys | 1,000 |

Each dimension is built in two stages, following the Lakeflow pattern:
1. A streaming staging table (`*_stg`) reads incrementally from the Silver layer via `spark.readStream.table(...)`
2. An **Auto CDC flow** (`dlt.create_auto_cdc_flow`) applies the staged changes onto an empty streaming target table, producing full SCD Type 2 history (`start_at` / `end_at` columns) automatically — no manual merge logic required

```python
import dlt

@dlt.table
def dimtrack_stg():
    return spark.readStream.table("spotify_cata.silver.dimtrack")

dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
    target="dimtrack",
    source="dimtrack_stg",
    keys=["track_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
```

---

## Repo Structure

```
databricks/spotify_dab/
├── databricks.yml                  # Bundle definition — dev/prod targets, variables, include paths
├── pyproject.toml
├── resources/
│   └── gold_pipeline.pipeline.yml  # Lakeflow pipeline definition (catalog, schema, source path)
├── source/gold/DT/transformations/ # Gold-layer DLT source files
│   ├── dim_user.py
│   ├── dim_track.py
│   ├── dim_date.py
│   └── factstream.py
├── src/spotify_dab/                # Bundle-managed Python package
├── utils/
└── requirements.txt
```

Deployed with:
```bash
databricks bundle deploy --target dev
```

---

## Challenges & How They Were Solved

A few real infrastructure problems came up building this — noting them here since they're the parts worth explaining in an interview.

**1. Unity Catalog metastore had no root storage credential.**
Creating the metastore through the setup wizard (ADLS path + Access Connector ID) does *not* automatically create a linked Storage Credential object — a known gap since a Unity Catalog platform change. This caused every pipeline run to fail with `DAC_DOES_NOT_EXIST`. Fixed by identifying the existing credential already used by the External Locations (`credential03`) and explicitly patching it onto the metastore:
```bash
databricks metastores update <metastore-id> --storage-root-credential-id <credential-id>
```

**2. Workspace UI couldn't create ETL pipelines.**
The "New → ETL Pipeline" button consistently failed with a generic directory-contents error, with no fix found in the UI. Worked around it entirely by defining the pipeline as YAML (`gold_pipeline.pipeline.yml`) inside a Databricks Asset Bundle and deploying via CLI instead — which also meant the pipeline definition was version-controlled and reproducible, not just clicked together in a UI.

**3. A missing `include:` directive silently broke the whole pipeline.**
After enabling `source_linked_deployment: false` (to make deployments copy files rather than reference live workspace paths) and clearing stale deployment state, the pipeline disappeared entirely from the backend — `databricks pipelines list-pipelines` returned empty, despite all source files being intact. Root cause: `databricks.yml` was missing its `include: - resources/*.yml` block, so the bundle no longer knew the pipeline definition existed. Re-adding it and redeploying recreated the pipeline cleanly with no data loss, since Silver-layer data (the actual source of truth) was untouched throughout.

---

## Related Repo Contents

- `/dataset`, `/factory`, `/linkedService`, `/pipeline` — Azure Data Factory pipelines feeding the Bronze layer (incremental CDC ingestion, watermark pattern, backfill support)
