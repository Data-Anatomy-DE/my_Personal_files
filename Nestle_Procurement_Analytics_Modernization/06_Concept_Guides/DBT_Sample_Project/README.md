# Sample dbt Project — Illustrative Structure

This is a small, illustrative dbt project showing the real file layout and syntax discussed in
[`../01_DBT_Introduction_and_Setup.md`](../01_DBT_Introduction_and_Setup.md) — built against the actual
sample data in [`../../04_Data/csv`](../../04_Data/csv) (`purchase_orders.csv`, `supplier_master.csv`).

It is intentionally minimal (two staging models, two mart models, one snapshot, one macro) — enough to
see real folder structure, real YAML, and real `ref()`/`source()` usage, not a full production project.

**Not included on purpose:** `profiles.yml` and `target/`. `profiles.yml` holds connection credentials and
should never be committed to version control (see Guide 01, section 4) — it lives outside the project, in
`~/.dbt/profiles.yml`, on whichever machine actually runs this. `target/` is a build artifact dbt
regenerates every run.

```
DBT_Sample_Project/
├── dbt_project.yml
├── models/
│   ├── staging/
│   │   ├── _staging__sources.yml     # declares the raw sources
│   │   ├── stg_suppliers.sql
│   │   └── stg_purchase_orders.sql
│   └── marts/
│       ├── _marts__schema.yml        # docs + tests for the marts
│       ├── dim_supplier.sql
│       └── fact_procurement_spend.sql # incremental model example
├── snapshots/
│   └── supplier_snapshot.sql         # SCD Type 2 example on risk_tier
└── macros/
    └── surrogate_key.sql             # small reusable Jinja macro
```

## How the pieces connect

`stg_purchase_orders` and `stg_suppliers` read directly from `source()` (the raw tables). `dim_supplier`
and `fact_procurement_spend` read from those staging models via `ref()` — never from each other's raw
sources directly, and never from a hardcoded table name. That chain is what lets dbt build the DAG:

```
source(raw_procurement.supplier_master)  ──▶ stg_suppliers ──▶ dim_supplier ──┐
                                                                                ├──▶ fact_procurement_spend
source(raw_procurement.purchase_orders)  ──▶ stg_purchase_orders ─────────────┘
```

`supplier_snapshot` reads the raw source directly (snapshots capture the *upstream* history, before any
staging cleanup) and uses the `check` strategy against `risk_tier`/`supplier_status`/`preferred_currency` —
so a supplier's risk-tier change produces a new historical row instead of overwriting the old one.
