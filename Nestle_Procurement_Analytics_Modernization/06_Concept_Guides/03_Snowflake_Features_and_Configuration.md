# Snowflake — Features Overview & Configuration Guide

## 1. What is Snowflake?

Snowflake is a cloud-native data platform (available on AWS, Azure, and GCP) delivered as a fully-managed SaaS — there's no infrastructure to patch, tune, or scale manually. Its defining architectural idea is separating **storage**, **compute**, and **cloud services** into independent layers, so you can scale any one of them without touching the others.

## 2. Key features, at a glance

- **Multi-cluster shared data architecture** — many independent virtual warehouses can query the same underlying data concurrently without contention.
- **Virtual Warehouses** — compute clusters (sized XS–6XL) that can auto-suspend/auto-resume and scale independently; billed per-second while running.
- **Micro-partitioning & automatic clustering** — data is automatically split into small, columnar, metadata-rich chunks so the optimizer can skip irrelevant data without you managing indexes.
- **Time Travel & Fail-safe** — query/restore data as it existed in the past (up to 90 days), plus a further Snowflake-managed recovery window.
- **Zero-copy cloning** — instantly clone a database/schema/table (metadata-only at first) for dev/test environments without duplicating storage.
- **Streams & Tasks** — native change-data-capture and scheduling, for building ELT pipelines without external orchestration tools.
- **Semi-structured data support** — the `VARIANT` type lets you load and query JSON/Avro/Parquet natively, alongside `FLATTEN()` to explode nested structures into rows.
- **Secure Data Sharing** — share live data with other Snowflake accounts (or non-Snowflake consumers) without copying or moving it.
- **Dynamic Tables** — declarative, auto-refreshing tables defined by a query plus a target freshness (`TARGET_LAG`), without hand-building Streams/Tasks.
- **Snowflake Cortex** — LLM and AI functions callable directly from SQL (summarize, translate, classify, semantic search) without moving data out of the platform.
- **Role-Based Access Control (RBAC)** — fine-grained privilege model; access is granted to roles, roles to users, and roles can inherit from other roles.

## 3. How to configure Snowflake — step by step

### 3.1 Warehouses, databases, and schemas

```sql
-- as ACCOUNTADMIN or SYSADMIN
CREATE WAREHOUSE TRANSFORM_WH
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60          -- seconds of inactivity before suspending
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE PROCUREMENT_DB;

CREATE SCHEMA PROCUREMENT_DB.RAW;
CREATE SCHEMA PROCUREMENT_DB.STAGING;
CREATE SCHEMA PROCUREMENT_DB.ANALYTICS;
```

A common, sane organizing convention: `RAW` (landed as-is), `STAGING` (lightly cleaned, 1:1 with sources), `ANALYTICS` (final modeled facts/dimensions) — this maps directly onto how a dbt project is structured (Guide 01).

### 3.2 Roles & access (don't run everything as ACCOUNTADMIN)

```sql
CREATE ROLE TRANSFORM_ROLE;

GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON DATABASE PROCUREMENT_DB TO ROLE TRANSFORM_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE PROCUREMENT_DB TO ROLE TRANSFORM_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA PROCUREMENT_DB.RAW TO ROLE TRANSFORM_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA PROCUREMENT_DB.STAGING TO ROLE TRANSFORM_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA PROCUREMENT_DB.ANALYTICS TO ROLE TRANSFORM_ROLE;

CREATE USER DBT_SERVICE_USER
  PASSWORD = '...'
  DEFAULT_ROLE = TRANSFORM_ROLE
  DEFAULT_WAREHOUSE = TRANSFORM_WH
  DEFAULT_NAMESPACE = 'PROCUREMENT_DB.STAGING';

GRANT ROLE TRANSFORM_ROLE TO USER DBT_SERVICE_USER;
```

The principle: grant privileges to **roles**, not directly to users, and grant roles to users — so access changes are managed by re-assigning roles, not hunting down every individual grant a person has.

### 3.3 Cost governance

```sql
CREATE RESOURCE MONITOR MONTHLY_ETL_BUDGET
  WITH CREDIT_QUOTA = 500
  TRIGGERS
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = MONTHLY_ETL_BUDGET;
```

### 3.4 Loading data in (recap)

Data typically lands via an external **stage** (pointing at S3/Azure Blob/GCS — see Guide 04 for the full S3 setup) and a `COPY INTO` statement, or continuously via **Snowpipe**. See the interview Q&A document for the deeper mechanics of `COPY INTO` error handling.

### 3.5 A sensible baseline structure

```
PROCUREMENT_DB
├── RAW           -- landed exactly as received, minimal transformation
├── STAGING        -- lightly cleaned/typed, ~1:1 with raw sources
└── ANALYTICS      -- final star-schema facts & dimensions for BI/reporting
```

Paired with `DEV`/`PROD` database-level (or schema-level) separation so developers can safely build and test without touching production tables.

## 4. Why teams pick Snowflake over "just running Postgres/a bigger server"

The separated storage/compute model means you don't provision for peak load all day — a huge nightly transformation job can spin up a large warehouse for 20 minutes and suspend immediately, while a small BI warehouse serves dashboards all day at a fraction of the cost, both against the exact same data with zero contention between them. That elasticity, combined with near-zero operational maintenance (no indexes to tune, no vacuuming, automatic scaling), is the core pitch.
