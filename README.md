# Unity Catalog on Azure Databricks — Medallion Lakehouse

End-to-end reference implementation of a **Unity Catalog-governed Medallion Lakehouse** on Azure Databricks backed by **Azure Data Lake Storage Gen2 (ADLS Gen2)**.

---

## Architecture Overview

```
  ADLS Gen2  ── dbstoragefinance
  ┌──────────────────────────────────────────────────────────┐
  │  bronze/          silver/          gold/                  │
  │  (raw JSON)       (cleansed Delta) (aggregated Delta)     │
  └──────────────────────────────────────────────────────────┘
        │                   │                   │
  ┌─────▼───────────────────▼───────────────────▼───────────┐
  │               Unity Catalog: finance_dev                  │
  │  bronze_dev         silver_dev         gold_dev           │
  │  (managed Delta)    (managed Delta)    (managed Delta)    │
  └─────────────────────────────────────────────────────────┘
```

The pipeline follows the **Medallion Architecture**:

| Layer | Schema | Pattern | Format |
|-------|--------|---------|--------|
| Bronze | `bronze_dev` | Auto Loader (incremental) | Delta (managed) |
| Silver | `silver_dev` | MERGE INTO (upsert) | Delta (managed, partitioned) |
| Gold | `gold_dev` | CREATE OR REPLACE (aggregation) | Delta (managed, partitioned) |

---

## Infrastructure

| Resource | Value |
|---|---|
| Storage Account | `dbstoragefinance` (ADLS Gen2) |
| ABFS Containers | `bronze`, `silver`, `gold`, `finance-dev` |
| Storage Credential | `dbstoragefinancestoragetoken` |
| UC Catalog | `finance_dev` |
| UC Schemas | `bronze_dev`, `silver_dev`, `gold_dev` |
| Dataset | Formula 1 — `drivers.json`, `results.json` |

---

## Notebook Sequence

Run notebooks in order. In a **Databricks Workflow**, chain them so each notebook passes control to the next.

### [0.config.ipynb](0.config.ipynb) — Centralized Configuration
- Declares **Databricks Widgets** for all configurable parameters: storage account, credential name, catalog name, environment tag.
- Exposes Python variables (`BRONZE_PATH`, `SILVER_PATH`, `GOLD_PATH`, `fq()` helper, etc.) that every downstream notebook consumes via `%run ./0.config`.
- **Override at runtime** via Databricks Job UI or `dbutils.widgets.text()` — no code changes needed to switch environments.

### [1.create_external_locations.ipynb](1.create_external_locations.ipynb) — External Locations
- Registers 4 ADLS Gen2 containers as Unity Catalog **External Locations** using the shared storage credential.
- Validates each location with `DESC EXTERNAL LOCATION`.
- Smoke-tests end-to-end connectivity by reading raw JSON directly from the Bronze container.

### [2.create_catalog_schema.ipynb](2.create_catalog_schema.ipynb) — Catalog & Schema Provisioning
- Creates the `finance_dev` **Unity Catalog** with its managed root in the `finance-dev` container.
- Creates `bronze_dev`, `silver_dev`, `gold_dev` schemas, each pinned to its own ADLS container.
- Validates catalog and schema metadata.

### [3.Create_Bronze_Table.ipynb](3.Create_Bronze_Table.ipynb) — Bronze Layer
- Creates **managed Delta tables** (`drivers`, `results`) with explicit DDL including `NOT NULL` constraints and `PARTITIONED BY (ingestion_date)`.
- Uses **Auto Loader (`cloudFiles`)** for incremental, checkpoint-backed ingestion — only new files processed on each run; safe to re-run.
- Adds `ingestion_date` and `source_file` audit columns automatically.
- Enables **Change Data Feed** (`delta.enableChangeDataFeed`) for downstream CDC patterns.
- Runs row count and NULL-key **data quality assertions** before exiting.

### [4.create managed table in silver schema.ipynb](4.create%20managed%20table%20in%20silver%20schema.ipynb) — Silver Layer
- Creates `drivers` and `results` Silver tables with full DDL, `NOT NULL` constraints, and partitioning (`nationality` / `race_id`).
- **`MERGE INTO` (upsert)** — replaces the old `DROP + CTAS` pattern; only inserts new rows or updates changed rows, making the pipeline idempotent and scalable.
- Transforms raw fields: flattens nested `name` struct, renames to snake_case, casts types.
- Compares Silver vs Bronze row counts as a **cross-layer DQ assertion**.

### [5.create gold tables.ipynb](5.create%20gold%20tables.ipynb) — Gold Layer
- Builds `driver_wins` — a fully aggregated reporting table (driver name, nationality, win count).
- `CREATE OR REPLACE TABLE` is intentional here — aggregations must be full recalculations.
- **Partitioned by `nationality`** for efficient BI filtering.
- DQ validation asserts non-empty output and no zero-or-negative win counts.

---

## Key Design Decisions

### Managed Delta Throughout
All Bronze, Silver, and Gold tables are **Unity Catalog managed Delta tables**. Unity Catalog owns both metadata and data lifecycle, enabling consistent `GRANT`/`REVOKE`, lineage, and audit trail across all layers.

### Incremental Loading
- **Bronze**: Auto Loader processes only new JSON files per run via checkpoints stored in `_checkpoint/` on ADLS.
- **Silver**: `MERGE INTO` upserts only changed or new rows, avoiding full table scans on large datasets.
- **Gold**: Full recalculation is correct for aggregation tables — `CREATE OR REPLACE` is efficient when Silver is small relative to Gold consumers' query patterns.

### Parameterization
All storage paths, credential names, catalog/schema identifiers, and environment tags are defined as **Databricks Widgets** in `0.config.ipynb`. No string literals appear in layer notebooks.

### Partitioning Strategy
| Table | Partition Key | Rationale |
|---|---|---|
| `bronze.drivers` | `ingestion_date` | Time-travel and incremental query pruning |
| `bronze.results` | `ingestion_date` | Time-travel and incremental query pruning |
| `silver.drivers` | `nationality` | BI filters heavily on nationality |
| `silver.results` | `race_id` | Results queried race-by-race |
| `gold.driver_wins` | `nationality` | Nationality-scoped leaderboard queries |

### Data Quality Gates
Each layer asserts:
- **Row count > 0** — table not empty after load
- **Primary key NOT NULL** — no orphaned records
- **`ingestion_date` NOT NULL** — audit trail intact
- **Silver = Bronze row counts** — no silent data loss in transformation

---

## Running Locally (VS Code + Databricks Extension)
1. Install the [Databricks extension for VS Code](https://docs.databricks.com/dev-tools/vscode-ext.html).
2. Configure your Databricks workspace connection.
3. Open any `.ipynb` notebook and run cells via the Databricks remote kernel.
4. Set Widget values via the `dbutils.widgets` API in the first cell to override defaults.

## Running as a Databricks Workflow
Create a multi-task Workflow in Databricks with the following task chain:

```
config → create_external_locations → create_catalog_schema
       → Create_Bronze_Table → silver_layer → gold_layer
```

Each task uses `%run ./0.config` to inherit Widget parameters passed from the Job run configuration.

---

## Repository Structure

```
unitycatalogAzureDatabrick/
├── 0.config.ipynb                             # Centralized configuration (Widgets)
├── 1.create_external_locations.ipynb          # Register ADLS containers in UC
├── 2.create_catalog_schema.ipynb              # Provision catalog & schemas
├── 3.Create_Bronze_Table.ipynb                # Raw ingestion (Auto Loader → Delta)
├── 4.create managed table in silver schema.ipynb  # Cleansing (MERGE INTO)
├── 5.create gold tables.ipynb                 # Aggregation (driver_wins)
└── README.md
```

