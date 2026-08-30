# dbt (Data Build Tool) — Introduction & Setup Guide

## 1. What is dbt, and why does it exist?

dbt is the **"T" in ELT** — a tool that lets you write your data transformations as plain SQL `SELECT` statements, and handles everything around them: dependency ordering, testing, documentation, and environment management. It doesn't extract or load data (that's handled by ingestion tools like Fivetran, Snowpipe, or custom scripts); it takes data that's already landed in your warehouse and transforms raw tables into clean, modeled, analytics-ready tables.

Before dbt, transformation logic usually lived in one of two uncomfortable places: giant stored procedures nobody wanted to touch, or SQL scripts scattered across an orchestrator with no tests, no version control discipline, and no easy way to see what depends on what. dbt brings standard software-engineering practices — version control, code review, modularity, automated testing, and generated documentation — to that SQL layer.

**Analogy:** if the data warehouse is a kitchen and raw data is groceries, dbt is the recipe book *and* the sous-chef who follows it in the right order, checks each dish before it goes out (tests), and keeps notes on how every dish was made (docs) — instead of one overworked cook improvising from memory every night.

## 2. dbt Core vs. dbt Cloud

- **dbt Core**: the open-source command-line tool. You run it yourself (locally, in CI, in an orchestrator like Airflow). Free, full control, more setup.
- **dbt Cloud**: a managed service (scheduler, IDE, hosted docs, admin UI) built on top of dbt Core. Easier to get a team running, but a paid product.

This guide covers **dbt Core**, installed from scratch.

## 3. Installing dbt Core from scratch

dbt Core is a Python package, and — importantly — installing `dbt-core` alone does **not** give you the ability to connect to any specific warehouse. You also need the adapter package for your platform.

```bash
# 1. Create an isolated Python environment (recommended, avoids version conflicts)
python -m venv dbt-env

# 2. Activate it
# macOS/Linux:
source dbt-env/bin/activate
# Windows (PowerShell):
dbt-env\Scripts\Activate.ps1

# 3. Install dbt-core AND the Snowflake adapter together
pip install dbt-core dbt-snowflake

# 4. Verify
dbt --version
```

`dbt --version` should show both `dbt-core` and `dbt-snowflake` with matching compatible versions. If you only installed `dbt-core`, `dbt` will run but will have no idea how to talk to Snowflake — this trips up almost everyone the first time.

## 4. Connecting dbt to Snowflake — `profiles.yml`

dbt stores *connection* details separately from the *project* itself, in a file called `profiles.yml`. This file lives outside your project folder — by default at `~/.dbt/profiles.yml` (macOS/Linux) or `%USERPROFILE%\.dbt\profiles.yml` (Windows) — specifically so connection secrets are never accidentally committed to git alongside your project code.

```yaml
# ~/.dbt/profiles.yml
procurement_analytics:              # this name must match `profile:` in dbt_project.yml
  target: dev
  outputs:
    dev:
      type: snowflake
      account: xy12345.ap-south-1   # your Snowflake account locator/region
      user: DBT_SERVICE_USER
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"   # never hardcode secrets
      role: TRANSFORM_ROLE
      database: PROCUREMENT_DB
      warehouse: TRANSFORM_WH
      schema: DBT_DEV_JMOHAMMED
      threads: 4
```

A few things worth understanding, not just copying:
- `threads` controls how many models dbt builds **in parallel** (respecting the dependency graph) — higher isn't always better, it's bounded by your warehouse size and how independent your models actually are.
- Using `{{ env_var(...) }}` instead of a literal password is standard practice — the password lives in an environment variable, not in a file that could end up in version control.
- Once this file exists, run `dbt debug` from inside your project folder — it checks the connection and reports exactly what's wrong if it fails (bad account locator, wrong role, network policy blocking, etc.), which is much more useful than a raw driver error.

## 5. Creating a new project

```bash
dbt init procurement_analytics
```

This asks a few interactive questions (which adapter, then the same connection details as above) and writes both the project folder **and** an entry into `profiles.yml` for you.

## 6. The folders `dbt init` creates, and what each one is for

```
procurement_analytics/
├── dbt_project.yml
├── models/
├── seeds/
├── snapshots/
├── tests/
├── macros/
├── analyses/
├── logs/
├── target/
├── .gitignore
└── README.md
```

| Folder / File | Purpose |
|---|---|
| **`dbt_project.yml`** | The project's master config: its name, which `profiles.yml` profile to use, default materializations, and file paths. This is *the* file dbt reads first. |
| **`models/`** | The heart of the project — your transformation SQL files. Each `.sql` file is one model (one `SELECT`, no need to write `CREATE TABLE` yourself). Convention (not auto-created, but near-universal) is to organize this into `staging/` (light cleanup of raw sources, roughly 1:1 with source tables), `intermediate/` (reusable building-block logic), and `marts/` (final, business-facing fact/dimension tables) — e.g., `models/staging/stg_purchase_orders.sql` → `models/marts/fact_procurement_spend.sql`. |
| **`seeds/`** | Small, static CSV files you want loaded as tables via `dbt seed` — e.g., a manually maintained country-code or currency-code lookup. Not for large or frequently changing data. |
| **`snapshots/`** | Definitions for capturing **SCD Type 2** history over time from a source that only exposes current state (e.g., snapshotting `raw.vendors` so you can see a vendor's risk-tier history, not just today's value). |
| **`tests/`** | **Singular tests** — one-off `.sql` files, each a custom assertion query that should return zero rows if the rule holds (generic, reusable tests like `not_null`/`unique` instead live in YAML next to the model). |
| **`macros/`** | Reusable Jinja+SQL snippets (like functions) — e.g., a standard way to build a surrogate key hash, used consistently across every model instead of copy-pasted with small variations. |
| **`analyses/`** | Ad hoc SQL you want dbt to compile (so it can use `ref()`/Jinja) but never actually materialize as a table/view in the warehouse — one-off investigative queries. |
| **`logs/`** | Auto-generated run logs. Not something you hand-edit. |
| **`target/`** | Auto-generated compiled output — the actual SQL dbt ran after resolving all `ref()`/`source()`/Jinja, plus `manifest.json` (the full project graph) and `run_results.json`. Normally `.gitignore`d — it's a build artifact, not source code. |
| **`.gitignore`** | Pre-populated to exclude `target/`, `dbt_packages/`, and log files from version control. |
| **`README.md`** | Project-level documentation starting point. |

Two folders you'll likely add yourself: `models/staging`, `models/marts` (and sometimes `models/intermediate`) as subfolders once you start organizing more than a handful of models, and a `packages.yml` at the project root if you want to pull in community packages (most commonly `dbt_utils`, for extra generic tests and helper macros).

## 7. YAML files in dbt — what they are and why so many

YAML (`.yml`) is a human-readable, indentation-sensitive config format — dbt uses it everywhere config needs to be declared *without* mixing config into SQL. The main ones you'll touch:

- **`dbt_project.yml`** — project-wide settings (paths, default materializations per folder).
- **A `.yml` file next to your models** (e.g., `models/staging/_staging.yml`) — this is where you: declare **sources** (raw tables dbt doesn't manage but needs to reference), document models and columns, and attach **generic tests** to columns.
- **`profiles.yml`** — connection details (see section 4) — lives outside the project on purpose.
- **`packages.yml`** — declares external dbt package dependencies, e.g.:
  ```yaml
  packages:
    - package: dbt-labs/dbt_utils
      version: [">=1.0.0", "<2.0.0"]
  ```

Example of a model/source YAML file:
```yaml
version: 2

sources:
  - name: raw_procurement
    database: PROCUREMENT_DB
    schema: RAW
    tables:
      - name: purchase_orders
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}

models:
  - name: stg_purchase_orders
    description: "One row per purchase order line, lightly cleaned from raw."
    columns:
      - name: po_line_id
        tests: [unique, not_null]
      - name: vendor_id
        tests:
          - relationships:
              to: ref('dim_vendor')
              field: vendor_id
```

## 8. `ref()` and `source()` — the two most important functions in dbt

- **`source()`** points at a **raw** table dbt does not manage — declared once in a YAML `sources:` block (as above), then referenced in SQL as `{{ source('raw_procurement', 'purchase_orders') }}`. This indirection is what powers `dbt source freshness` (checking whether upstream data has actually landed recently).
- **`ref()`** points at **another dbt model** — `{{ ref('stg_purchase_orders') }}` instead of hardcoding `PROCUREMENT_DB.DBT_DEV_JMOHAMMED.stg_purchase_orders`.

Why this matters so much: every `ref()`/`source()` call is how dbt automatically builds the **DAG** (dependency graph) — it knows `fact_procurement_spend` depends on `stg_purchase_orders`, so it always builds things in the right order, and can run just a model plus everything downstream of it (`dbt run --select stg_purchase_orders+`). It's also what makes an environment swap free: the *same* model code, using `ref()`, resolves to a developer's own dev schema in dev and to the shared prod schema in prod — no find-and-replace of hardcoded table names required.

```sql
-- models/marts/fact_procurement_spend.sql
select
    po.po_line_id,
    po.vendor_id,
    v.vendor_name,
    po.line_amount,
    po.order_date
from {{ ref('stg_purchase_orders') }} po
left join {{ ref('dim_vendor') }} v
    on po.vendor_id = v.vendor_id
```

## 9. Running the project

| Command | What it does |
|---|---|
| `dbt run` | Builds all models (or a selected subset with `--select`). |
| `dbt test` | Runs all generic + singular tests. |
| `dbt build` | Runs models, tests, snapshots, and seeds together, **in DAG order**, skipping downstream models if an upstream test fails — the safest single command for CI. |
| `dbt seed` | Loads CSVs from `seeds/` into the warehouse. |
| `dbt snapshot` | Runs snapshot definitions (SCD2 capture). |
| `dbt docs generate` + `dbt docs serve` | Builds and opens the auto-generated documentation site and lineage graph. |

## 10. Putting it together

dbt's value isn't any single command — it's that `models/` (the logic), the YAML files (tests, docs, sources), and `ref()`/`source()` (the dependency graph) combine into a project that's testable, self-documenting, safely re-runnable, and portable between dev and prod without manual find-and-replace. That combination is why teams that used to have "a folder of SQL scripts someone runs in the right order manually" moved to dbt — the right order, the tests, and the documentation are no longer things you have to remember.
