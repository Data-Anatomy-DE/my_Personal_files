# Tech Stack & Integration Reference Guide
### Nestle Procurement Analytics Modernization — Training Batch

This guide explains **each tool in the stack**, **why it's used**, and — most importantly — **how they connect to each other**. Read this alongside the Master Playbook. Concrete commands and code live in `03_Code_Reference/Code_Reference_Guide.md`.

---

## 1. AWS S3 — The Data Lake / Landing Zone

**What it is:** Object storage. Think of it as a very large, durable folder structure in the cloud. Each file (CSV, JSON, Parquet, etc.) is an "object" stored in a "bucket."

**Role in this project:** S3 is where the (simulated) API/Integration team drops raw procurement extracts before Snowflake ever sees them. It is the landing zone / data lake layer, decoupled from the warehouse.

**Recommended bucket/prefix structure for this training project:**

```
s3://nestle-procurement-raw/
   supplier/
      supplier_master/dt=2026-08-17/supplier_master.csv
      supplier_onboarding/dt=2026-08-17/supplier_onboarding.csv
      supplier_rating/dt=2026-08-17/supplier_rating.csv
   materials/
      product_master/dt=2026-08-17/product_master.csv
      plant_master/dt=2026-08-17/plant_master.csv
   finance/
      currency_exchange_rate/dt=2026-08-17/currency_exchange_rate.csv
   procurement/
      purchase_orders/dt=2026-08-17/purchase_orders.csv
      purchase_order_lines_nested/dt=2026-08-17/purchase_order_lines_nested.json
   accounts_payable/
      invoices/dt=2026-08-17/invoices.csv
      payments/dt=2026-08-17/payments.csv
   logistics/
      shipment_tracking/dt=2026-08-17/shipment_tracking.csv
      shipment_tracking_events/dt=2026-08-17/shipment_tracking_events.json
```

Partitioning by `dt=` (date) mirrors how real extract jobs land daily/incremental files, and makes it trivial for Snowflake or Airflow to pick up "today's" files only.

**Key concepts candidates must know:**
- **Bucket** — top-level container (globally unique name)
- **Prefix** — the "folder path" inside a bucket (S3 has no real folders, just key prefixes)
- **IAM Role / Policy** — controls who/what can read from the bucket (Snowflake needs a role that can `s3:GetObject` and `s3:ListBucket`)
- **Versioning** (optional) — keeps history of overwritten files, useful for auditability

---

## 2. Snowflake — The Cloud Data Platform

**What it is:** A cloud data warehouse that separates storage and compute. You load data into tables, then query/transform it using independently scalable "virtual warehouses" (compute clusters).

**Role in this project:** Snowflake hosts the entire medallion architecture — Bronze (RAW), Silver (STAGING), Gold (MART) — as three schemas inside one database, e.g. `NESTLE_PROCUREMENT_DB.RAW`, `...STAGING`, `...MART`.

**Key objects candidates must know:**

| Object | Purpose |
|---|---|
| **Warehouse** | Compute engine that runs your queries/loads (e.g. `LOAD_WH`, `TRANSFORM_WH`) |
| **Database / Schema** | Logical containers — one schema per medallion layer |
| **File Format** | Tells Snowflake how to parse a file (CSV delimiter, header row, JSON strip-outer-array, etc.) |
| **Storage Integration** | A secure, credential-less connection object between Snowflake and your S3 bucket (uses an AWS IAM role, not access keys) |
| **External Stage** | A named pointer to an S3 location (bucket + prefix), created using the storage integration |
| **`COPY INTO`** | Bulk-loads files from a stage into a table (batch loading) |
| **Snowpipe** | Automates `COPY INTO` continuously as new files land in S3 (near-real-time loading, triggered by S3 event notifications) |
| **`VARIANT`** | A column type that stores semi-structured data (JSON) as-is |
| **`FLATTEN`** | A table function that explodes nested arrays/objects inside a `VARIANT` column into relational rows |

### 2.1 How S3 Connects to Snowflake (step-by-step, conceptually)

1. **Create a Storage Integration** in Snowflake — this generates an AWS IAM entity (external ID + ARN) that Snowflake will use.
2. **Update the AWS IAM Role's trust policy** to allow Snowflake's IAM user to assume it (copy the ARN/external ID Snowflake gives you into your AWS IAM role trust relationship).
3. **Create a File Format** in Snowflake matching your file type (CSV with header, or JSON).
4. **Create an External Stage** referencing the storage integration + the S3 URL (bucket/prefix) + file format.
5. **Run `COPY INTO <bronze_table> FROM @external_stage`** to bulk load, or configure **Snowpipe** with an S3 event notification for near-real-time ingestion.

This is the "no static AWS keys stored in Snowflake" pattern — the recommended, secure way to connect the two systems. See Code Reference Guide Section 1 for the exact SQL.

### 2.2 Handling the Semi-Structured JSON Files

`purchase_order_lines_nested.json` and `shipment_tracking_events.json` are both **arrays of nested objects** (line items / approval steps; tracking events). The pattern for both is the same:

1. Load the whole JSON document into a single `VARIANT` column in a Bronze table (one row per top-level array element, using `file_format = json, strip_outer_array = true`).
2. In a dbt staging model, use `LATERAL FLATTEN(input => bronze_column, path => 'line_items')` (or `'tracking_events'`, `'approval_history'`) to explode the nested array into one row per line item / event / approval step.
3. Extract fields out of the flattened `VALUE` column using `:field_name` notation (e.g. `value:material_id::string`).

This is the core "handling semi-structured data" lesson for the batch — see Code Reference Guide Section 3.2 for the full flatten query.

---

## 3. dbt (data build tool) — Transformation Layer

**What it is:** A SQL-first transformation framework. You write `SELECT` statements ("models"), dbt compiles and runs them as views/tables in Snowflake, and manages dependencies between them automatically (the DAG/lineage).

**Role in this project:** dbt turns Bronze tables into Silver (staging) and then Gold (mart: facts & dimensions).

**Key concepts:**

| Concept | Purpose |
|---|---|
| `models/staging/` | One `stg_*.sql` model per Bronze source table — cleansing, renaming, typing, flattening JSON |
| `models/marts/` | `dim_*.sql` and `fct_*.sql` models — the star schema, built on top of staging models |
| `sources.yml` | Declares the Bronze tables dbt is allowed to read, enabling freshness checks and lineage |
| `schema.yml` | Declares tests (`not_null`, `unique`, `relationships`, `accepted_values`) and documentation for each model |
| `ref()` | How one dbt model refers to another (e.g. `{{ ref('stg_purchase_orders') }}`) — this is what builds the dependency graph |
| `source()` | How a staging model refers to a raw Bronze table declared in `sources.yml` |
| Materializations | `view` (default, cheap) vs `table` (persisted, faster to query) vs `incremental` (append/merge only new data) |
| Snapshots | dbt's built-in mechanism for tracking Slowly Changing Dimensions (e.g. supplier attribute history) |
| Macros | Reusable SQL snippets (Jinja + SQL), e.g. a shared currency-conversion macro |
| `dbt run` | Executes models (builds views/tables) |
| `dbt test` | Executes the declared data quality tests |
| `dbt docs generate` | Builds an interactive lineage graph and documentation site |

**Typical project layout for this batch:**

```
nestle_procurement_dbt/
  models/
    staging/
      stg_supplier_master.sql
      stg_supplier_onboarding.sql
      stg_supplier_rating.sql
      stg_product_master.sql
      stg_plant_master.sql
      stg_currency_exchange_rate.sql
      stg_purchase_orders.sql
      stg_purchase_order_lines_nested.sql   -- FLATTEN
      stg_invoices.sql
      stg_payments.sql
      stg_shipment_tracking.sql
      stg_shipment_tracking_events.sql      -- FLATTEN
      sources.yml
      schema.yml
    marts/
      dim_supplier.sql
      dim_material.sql
      dim_plant.sql
      dim_date.sql
      dim_currency.sql
      fct_purchase_order.sql
      fct_invoice.sql
      fct_payment.sql
      fct_shipment.sql
      fct_shipment_event.sql
      fct_supplier_rating.sql
      schema.yml
  dbt_project.yml
```

---

## 4. Apache Airflow — Orchestration

**What it is:** A Python-based workflow scheduler. Pipelines are defined as **DAGs** (Directed Acyclic Graphs) — a set of tasks with dependencies, run on a schedule.

**Role in this project:** Airflow is the "conductor" — it decides *when* and *in what order* everything runs: load Bronze → build Silver → build Gold → run tests → refresh Power BI.

**Key concepts:**

| Concept | Purpose |
|---|---|
| **DAG** | The overall pipeline definition (a Python file) |
| **Operator** | A single task type, e.g. `SnowflakeOperator` (run SQL), `BashOperator` (run `dbt run`), `PythonOperator` (call an API) |
| **Task Dependency** (`>>`) | Defines execution order, e.g. `load_bronze >> dbt_run_staging >> dbt_run_marts >> dbt_test >> refresh_powerbi` |
| **Schedule Interval** | How often the DAG runs (e.g. daily at 6 AM) |
| **Sensor** | A task that waits for a condition, e.g. `S3KeySensor` waits for a file to land before continuing |
| **XCom** | Small pieces of data passed between tasks |
| **Connections** | Airflow's secure store for credentials (Snowflake connection, AWS connection, Power BI service principal) |

For this training project, the DAG is intentionally simple — a linear chain, not a complex fan-out — so the orchestration concept is clear before adding complexity. See Code Reference Guide Section 4 for the full DAG code.

---

## 5. Power BI — Semantic Layer & Visualization

**What it is:** A BI tool that connects to a data source, builds a semantic model (tables + relationships + measures), and renders interactive dashboards.

**Role in this project:** Power BI connects to the Gold-layer semantic views in Snowflake (`vw_supplier_scorecard`, `vw_otif_performance`, `vw_procure_to_pay`, etc.), builds a star-schema semantic model, and layers on DAX measures for the KPIs defined in the Playbook.

**Key concepts:**

| Concept | Purpose |
|---|---|
| **Connector** | Power BI's native Snowflake connector (Get Data → Snowflake) |
| **Import vs DirectQuery** | Import copies data into Power BI's in-memory engine (fast, needs refresh); DirectQuery queries Snowflake live (always current, slower) — for this training batch, **Import** is recommended for simplicity |
| **Star Schema Semantic Model** | Fact tables connected to dimension tables via one-to-many relationships, matching the dbt mart layer 1:1 |
| **Measures (DAX)** | Calculated KPIs, e.g. `OTIF % = DIVIDE([Shipments OTIF], [Total Shipments])` |
| **Dataset Refresh** | Re-pulls data from Snowflake; can be triggered manually, on a schedule, or via the Power BI REST API (used by the Airflow DAG) |

Sample DAX measures are in Code Reference Guide Section 5.

---

## 6. End-to-End Flow — Recap

```
1. API/Integration team extracts data from SAP ERP, Ariba/SRM, QMS, TMS
        → lands CSV/JSON files in S3 raw zone (partitioned by source/date)

2. Snowflake Storage Integration + External Stage point at the S3 zone
        → COPY INTO (or Snowpipe) loads Bronze/RAW tables — one per source file

3. dbt staging models clean/type/flatten Bronze → Silver
        → JSON files are flattened here using LATERAL FLATTEN

4. dbt mart models build the star schema: Silver → Gold
        → dim_supplier, dim_material, dim_plant, dim_date, dim_currency
        → fct_purchase_order, fct_invoice, fct_payment, fct_shipment,
          fct_shipment_event, fct_supplier_rating

5. dbt tests validate uniqueness, not-null, referential integrity

6. Snowflake semantic views wrap the Gold layer for BI consumption

7. Power BI connects to semantic views, builds the model, renders dashboards

8. Apache Airflow orchestrates steps 2–7 end-to-end on a schedule
```
