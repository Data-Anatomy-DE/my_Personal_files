# Code Reference Guide
### Nestle Procurement Analytics Modernization — Training Batch

This is a consolidated, ready-to-adapt code reference. It follows the same order as the Tech Stack Reference Guide: **Snowflake setup → Bronze load → dbt Silver → dbt Gold → Airflow DAG → Power BI DAX**. Column names match the sample files in `04_Data/csv` and `04_Data/json` exactly.

---

## 1. Snowflake Setup — Database, Warehouses, Storage Integration

```sql
-- 1.1 Database and schemas (medallion layers)
CREATE DATABASE IF NOT EXISTS NESTLE_PROCUREMENT_DB;
CREATE SCHEMA IF NOT EXISTS NESTLE_PROCUREMENT_DB.RAW;
CREATE SCHEMA IF NOT EXISTS NESTLE_PROCUREMENT_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS NESTLE_PROCUREMENT_DB.MART;

-- 1.2 Virtual warehouses
CREATE WAREHOUSE IF NOT EXISTS LOAD_WH  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- 1.3 Storage integration to S3 (credential-less, IAM role based)
CREATE STORAGE INTEGRATION nestle_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/nestle-snowflake-access-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nestle-procurement-raw/');

-- Run this, then copy the output STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID
-- into the trust policy of the AWS IAM role above.
DESC INTEGRATION nestle_s3_integration;

-- 1.4 File formats
CREATE OR REPLACE FILE FORMAT NESTLE_PROCUREMENT_DB.RAW.CSV_STANDARD
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'N/A');

CREATE OR REPLACE FILE FORMAT NESTLE_PROCUREMENT_DB.RAW.JSON_STANDARD
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE;

-- 1.5 External stages (one per source domain, or one generic stage pointed at bucket root)
CREATE OR REPLACE STAGE NESTLE_PROCUREMENT_DB.RAW.S3_PROCUREMENT_STAGE
  STORAGE_INTEGRATION = nestle_s3_integration
  URL = 's3://nestle-procurement-raw/'
  FILE_FORMAT = NESTLE_PROCUREMENT_DB.RAW.CSV_STANDARD;

-- Quick smoke test: list files Snowflake can see in the bucket
LIST @NESTLE_PROCUREMENT_DB.RAW.S3_PROCUREMENT_STAGE;
```

---

## 2. Bronze (RAW) Layer — Table DDL + COPY INTO

### 2.1 CSV tables

```sql
USE SCHEMA NESTLE_PROCUREMENT_DB.RAW;

CREATE OR REPLACE TABLE supplier_master (
  supplier_id             STRING,
  supplier_name           STRING,
  supplier_category       STRING,
  country                 STRING,
  region                  STRING,
  city                    STRING,
  preferred_currency      STRING,
  payment_terms_days      NUMBER,
  tax_id                  STRING,
  supplier_status         STRING,
  risk_tier               STRING,
  onboarded_date          DATE,
  primary_contact_email   STRING,
  _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE supplier_onboarding (
  onboarding_id                   STRING,
  supplier_id                     STRING,
  supplier_name                   STRING,
  onboarding_stage                STRING,
  compliance_check_fssc22000      STRING,
  compliance_check_iso9001        STRING,
  compliance_check_sedex          STRING,
  sustainability_score            NUMBER,
  bank_verification_status        STRING,
  onboarding_start_date           DATE,
  onboarding_completion_date      DATE,
  approved_by                     STRING,
  _loaded_at                      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE supplier_rating (
  rating_id                STRING,
  supplier_id               STRING,
  supplier_name             STRING,
  rating_period             STRING,
  on_time_delivery_pct      FLOAT,
  quality_rejection_pct     FLOAT,
  responsiveness_score      NUMBER,
  overall_rating            STRING,
  comments                  STRING,
  _loaded_at                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE product_master (
  material_id       STRING,
  material_name     STRING,
  material_category STRING,
  uom               STRING,
  standard_cost     FLOAT,
  currency          STRING,
  shelf_life_days   STRING,   -- kept STRING because raw source uses 'N/A' for non-perishables
  is_hazardous      STRING,
  _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE plant_master (
  plant_id     STRING,
  plant_name   STRING,
  plant_type   STRING,
  country      STRING,
  region       STRING,
  city         STRING,
  _loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE currency_exchange_rate (
  exchange_rate_id  STRING,
  currency_code     STRING,
  rate_month        DATE,
  rate_to_usd       FLOAT,
  _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE purchase_orders (
  po_id                     STRING,
  po_line_num               NUMBER,
  po_date                   DATE,
  supplier_id               STRING,
  plant_id                  STRING,
  material_id               STRING,
  order_qty                 NUMBER,
  uom                       STRING,
  unit_price                FLOAT,
  currency                  STRING,
  line_amount               FLOAT,
  requested_delivery_date   DATE,
  po_status                 STRING,
  buyer_name                STRING,
  payment_terms_days        NUMBER,
  _loaded_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE invoices (
  invoice_id         STRING,
  invoice_line_num   NUMBER,
  po_id              STRING,
  po_line_num        NUMBER,
  supplier_id        STRING,
  invoice_date       DATE,
  invoice_due_date   DATE,
  invoiced_qty       NUMBER,
  unit_price         FLOAT,
  currency           STRING,
  line_amount        FLOAT,
  tax_amount         FLOAT,
  total_amount       FLOAT,
  match_status       STRING,
  invoice_status     STRING,
  _loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE payments (
  payment_id                       STRING,
  invoice_id                       STRING,
  supplier_id                      STRING,
  payment_date                     DATE,
  payment_amount                   FLOAT,
  currency                         STRING,
  payment_method                   STRING,
  payment_status                   STRING,
  days_to_pay                      NUMBER,
  early_payment_discount_applied   STRING,
  _loaded_at                        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE shipment_tracking (
  shipment_id             STRING,
  po_id                   STRING,
  supplier_id             STRING,
  plant_id                STRING,
  carrier_name            STRING,
  mode_of_transport       STRING,
  origin_country          STRING,
  destination_plant       STRING,
  ship_date               DATE,
  estimated_arrival_date  DATE,
  actual_arrival_date     DATE,
  shipment_status         STRING,
  otif_flag               STRING,
  temperature_controlled  STRING,
  _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 2.2 JSON (VARIANT) tables

```sql
CREATE OR REPLACE TABLE shipment_tracking_events_raw (
  raw_json    VARIANT,
  _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE purchase_order_lines_nested_raw (
  raw_json    VARIANT,
  _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 2.3 COPY INTO — loading each table

```sql
COPY INTO supplier_master
  FROM @S3_PROCUREMENT_STAGE/supplier/supplier_master/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD')
  ON_ERROR = 'ABORT_STATEMENT';

COPY INTO supplier_onboarding
  FROM @S3_PROCUREMENT_STAGE/supplier/supplier_onboarding/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO supplier_rating
  FROM @S3_PROCUREMENT_STAGE/supplier/supplier_rating/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO product_master
  FROM @S3_PROCUREMENT_STAGE/materials/product_master/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO plant_master
  FROM @S3_PROCUREMENT_STAGE/materials/plant_master/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO currency_exchange_rate
  FROM @S3_PROCUREMENT_STAGE/finance/currency_exchange_rate/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO purchase_orders
  FROM @S3_PROCUREMENT_STAGE/procurement/purchase_orders/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO invoices
  FROM @S3_PROCUREMENT_STAGE/accounts_payable/invoices/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO payments
  FROM @S3_PROCUREMENT_STAGE/accounts_payable/payments/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

COPY INTO shipment_tracking
  FROM @S3_PROCUREMENT_STAGE/logistics/shipment_tracking/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

-- JSON loads (note: raw_json column, JSON_STANDARD format)
COPY INTO shipment_tracking_events_raw (raw_json)
  FROM @S3_PROCUREMENT_STAGE/logistics/shipment_tracking_events/
  FILE_FORMAT = (FORMAT_NAME = 'JSON_STANDARD')
  MATCH_BY_COLUMN_NAME = NONE;

COPY INTO purchase_order_lines_nested_raw (raw_json)
  FROM @S3_PROCUREMENT_STAGE/procurement/purchase_order_lines_nested/
  FILE_FORMAT = (FORMAT_NAME = 'JSON_STANDARD')
  MATCH_BY_COLUMN_NAME = NONE;
```

### 2.4 Snowpipe (near-real-time alternative to scheduled COPY INTO)

```sql
CREATE OR REPLACE PIPE NESTLE_PROCUREMENT_DB.RAW.PIPE_SHIPMENT_TRACKING
  AUTO_INGEST = TRUE
  AS
  COPY INTO shipment_tracking
  FROM @S3_PROCUREMENT_STAGE/logistics/shipment_tracking/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');

-- After creating the pipe, copy its notification_channel (SHOW PIPES) into an
-- S3 Event Notification on the bucket so new files auto-trigger the pipe.
SHOW PIPES LIKE 'PIPE_SHIPMENT_TRACKING';
```

---

## 3. Silver Layer — dbt Staging Models

### 3.1 `sources.yml`

```yaml
version: 2

sources:
  - name: raw
    database: NESTLE_PROCUREMENT_DB
    schema: RAW
    tables:
      - name: supplier_master
      - name: supplier_onboarding
      - name: supplier_rating
      - name: product_master
      - name: plant_master
      - name: currency_exchange_rate
      - name: purchase_orders
      - name: invoices
      - name: payments
      - name: shipment_tracking
      - name: shipment_tracking_events_raw
      - name: purchase_order_lines_nested_raw
```

### 3.2 Standard staging model — `stg_purchase_orders.sql`

```sql
-- models/staging/stg_purchase_orders.sql
with source as (
    select * from {{ source('raw', 'purchase_orders') }}
)

select
    po_id,
    po_line_num,
    po_date,
    supplier_id,
    plant_id,
    material_id,
    order_qty,
    uom,
    unit_price,
    currency,
    line_amount,
    requested_delivery_date,
    po_status,
    buyer_name,
    payment_terms_days
from source
where po_id is not null
```

### 3.3 Flattening JSON — `stg_purchase_order_lines_nested.sql`

This is the key semi-structured-data lesson. `purchase_order_lines_nested.json` has **two** nested arrays per record (`line_items` and `approval_history`) — flatten each separately.

```sql
-- models/staging/stg_purchase_order_lines_nested.sql
with source as (
    select raw_json from {{ source('raw', 'purchase_order_lines_nested_raw') }}
),

line_items_flat as (
    select
        source.raw_json:po_id::string            as po_id,
        source.raw_json:po_date::date             as po_date,
        source.raw_json:supplier_id::string       as supplier_id,
        source.raw_json:plant_id::string          as plant_id,
        source.raw_json:currency::string          as currency,
        line_item.value:po_line_num::number       as po_line_num,
        line_item.value:material_id::string       as material_id,
        line_item.value:material_name::string     as material_name,
        line_item.value:order_qty::number          as order_qty,
        line_item.value:uom::string                as uom,
        line_item.value:unit_price::float          as unit_price,
        line_item.value:line_amount::float         as line_amount
    from source,
         lateral flatten(input => source.raw_json:line_items) as line_item
)

select * from line_items_flat
```

```sql
-- models/staging/stg_purchase_order_approvals.sql
-- second flatten, over approval_history, kept as its own staging model
with source as (
    select raw_json from {{ source('raw', 'purchase_order_lines_nested_raw') }}
)

select
    source.raw_json:po_id::string          as po_id,
    step.value:step::number                as approval_step,
    step.value:approver_role::string       as approver_role,
    step.value:approver_name::string       as approver_name,
    step.value:action::string              as approval_action,
    step.value:action_date::date           as approval_action_date
from source,
     lateral flatten(input => source.raw_json:approval_history) as step
```

### 3.4 Flattening the shipment events JSON — `stg_shipment_tracking_events.sql`

```sql
-- models/staging/stg_shipment_tracking_events.sql
with source as (
    select raw_json from {{ source('raw', 'shipment_tracking_events_raw') }}
)

select
    source.raw_json:shipment_id::string             as shipment_id,
    source.raw_json:po_id::string                   as po_id,
    source.raw_json:carrier_name::string            as carrier_name,
    source.raw_json:mode_of_transport::string       as mode_of_transport,
    source.raw_json:temperature_controlled::boolean as temperature_controlled,
    event.value:event_seq::number                   as event_seq,
    event.value:event_timestamp::timestamp_ntz       as event_timestamp,
    event.value:location::string                     as location,
    event.value:event_status::string                 as event_status,
    event.value:temperature_c::float                  as temperature_c
from source,
     lateral flatten(input => source.raw_json:tracking_events) as event
```

### 3.5 Remaining staging models (same pattern as 3.2, one per Bronze table)

```sql
-- models/staging/stg_supplier_master.sql
select supplier_id, supplier_name, supplier_category, country, region, city,
       preferred_currency, payment_terms_days, tax_id, supplier_status, risk_tier,
       onboarded_date, primary_contact_email
from {{ source('raw', 'supplier_master') }}
where supplier_id is not null

-- models/staging/stg_invoices.sql
select invoice_id, invoice_line_num, po_id, po_line_num, supplier_id, invoice_date,
       invoice_due_date, invoiced_qty, unit_price, currency, line_amount, tax_amount,
       total_amount, match_status, invoice_status
from {{ source('raw', 'invoices') }}
where invoice_id is not null

-- models/staging/stg_payments.sql
select payment_id, invoice_id, supplier_id, payment_date, payment_amount, currency,
       payment_method, payment_status, days_to_pay,
       (early_payment_discount_applied = 'Y') as early_payment_discount_applied
from {{ source('raw', 'payments') }}
where payment_id is not null

-- models/staging/stg_shipment_tracking.sql
select shipment_id, po_id, supplier_id, plant_id, carrier_name, mode_of_transport,
       origin_country, destination_plant, ship_date, estimated_arrival_date,
       actual_arrival_date, shipment_status, (otif_flag = 'Y') as otif_flag,
       (temperature_controlled = 'Y') as temperature_controlled
from {{ source('raw', 'shipment_tracking') }}
where shipment_id is not null
```
*(`stg_supplier_onboarding`, `stg_supplier_rating`, `stg_product_master`, `stg_plant_master` and `stg_currency_exchange_rate` follow the identical select-and-rename pattern.)*

### 3.6 `schema.yml` tests for staging models (sample)

```yaml
version: 2

models:
  - name: stg_purchase_orders
    columns:
      - name: po_id
        tests: [not_null]
      - name: supplier_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_supplier_master')
              field: supplier_id
```

---

## 4. Gold Layer — dbt Mart Models (Facts & Dimensions)

### 4.1 Dimension — `dim_supplier.sql`

```sql
-- models/marts/dim_supplier.sql
with supplier as (
    select * from {{ ref('stg_supplier_master') }}
),
onboarding as (
    select * from {{ ref('stg_supplier_onboarding') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['supplier.supplier_id']) }} as supplier_key,
    supplier.supplier_id,
    supplier.supplier_name,
    supplier.supplier_category,
    supplier.country,
    supplier.region,
    supplier.city,
    supplier.preferred_currency,
    supplier.payment_terms_days,
    supplier.supplier_status,
    supplier.risk_tier,
    supplier.onboarded_date,
    onboarding.onboarding_stage,
    onboarding.sustainability_score,
    onboarding.bank_verification_status
from supplier
left join onboarding on supplier.supplier_id = onboarding.supplier_id
```

### 4.2 Dimension — `dim_date.sql` (generated, no source table)

```sql
-- models/marts/dim_date.sql
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2025-01-01' as date)",
    end_date="cast('2027-01-01' as date)"
   )
}}
```
Add downstream `select date_day, year(date_day) as year, quarter(date_day) as quarter, month(date_day) as month, dayofweek(date_day) as day_of_week from {{ this }}` in a second pass, or extend the CTE inline — kept minimal here for the training exercise.

### 4.3 Fact — `fct_purchase_order.sql`

```sql
-- models/marts/fct_purchase_order.sql
select
    po.po_id,
    po.po_line_num,
    po.po_date,
    supplier.supplier_key,
    po.plant_id,
    po.material_id,
    po.order_qty,
    po.uom,
    po.unit_price,
    po.currency,
    po.line_amount,
    po.requested_delivery_date,
    po.po_status,
    po.buyer_name
from {{ ref('stg_purchase_orders') }} po
left join {{ ref('dim_supplier') }} supplier
    on po.supplier_id = supplier.supplier_id
```

### 4.4 Fact — `fct_invoice.sql` (with 3-way match signal)

```sql
-- models/marts/fct_invoice.sql
select
    inv.invoice_id,
    inv.invoice_line_num,
    inv.po_id,
    inv.po_line_num,
    supplier.supplier_key,
    inv.invoice_date,
    inv.invoice_due_date,
    inv.invoiced_qty,
    inv.unit_price,
    inv.currency,
    inv.line_amount,
    inv.tax_amount,
    inv.total_amount,
    inv.match_status,
    inv.invoice_status,
    (inv.match_status = 'Matched') as is_clean_match
from {{ ref('stg_invoices') }} inv
left join {{ ref('dim_supplier') }} supplier
    on inv.supplier_id = supplier.supplier_id
```

### 4.5 Fact — `fct_shipment.sql` (with OTIF logic)

```sql
-- models/marts/fct_shipment.sql
select
    s.shipment_id,
    s.po_id,
    supplier.supplier_key,
    s.plant_id,
    s.carrier_name,
    s.mode_of_transport,
    s.origin_country,
    s.ship_date,
    s.estimated_arrival_date,
    s.actual_arrival_date,
    s.shipment_status,
    s.otif_flag,
    s.temperature_controlled,
    datediff('day', s.estimated_arrival_date, s.actual_arrival_date) as arrival_variance_days
from {{ ref('stg_shipment_tracking') }} s
left join {{ ref('dim_supplier') }} supplier
    on s.supplier_id = supplier.supplier_id
```

### 4.6 Remaining facts (`fct_payment`, `fct_shipment_event`, `fct_supplier_rating`)

```sql
-- models/marts/fct_payment.sql
select p.*, s.supplier_key
from {{ ref('stg_payments') }} p
left join {{ ref('dim_supplier') }} s on p.supplier_id = s.supplier_id

-- models/marts/fct_shipment_event.sql
select * from {{ ref('stg_shipment_tracking_events') }}

-- models/marts/fct_supplier_rating.sql
select r.*, s.supplier_key
from {{ ref('stg_supplier_rating') }} r
left join {{ ref('dim_supplier') }} s on r.supplier_id = s.supplier_id
```

### 4.7 Mart-layer `schema.yml` tests (sample)

```yaml
version: 2

models:
  - name: fct_invoice
    columns:
      - name: invoice_id
        tests: [not_null]
      - name: po_id
        tests:
          - relationships:
              to: ref('fct_purchase_order')
              field: po_id
      - name: match_status
        tests:
          - accepted_values:
              values: ['Matched', 'Mismatch-Qty', 'Mismatch-Price']
```

### 4.8 Semantic view for Power BI

```sql
-- Snowflake view on top of the Gold layer, purpose-built for Power BI
CREATE OR REPLACE VIEW NESTLE_PROCUREMENT_DB.MART.VW_SUPPLIER_SCORECARD AS
SELECT
    d.supplier_name,
    d.supplier_category,
    d.region,
    r.rating_period,
    r.on_time_delivery_pct,
    r.quality_rejection_pct,
    r.overall_rating
FROM NESTLE_PROCUREMENT_DB.MART.FCT_SUPPLIER_RATING r
JOIN NESTLE_PROCUREMENT_DB.MART.DIM_SUPPLIER d
    ON r.supplier_key = d.supplier_key;
```

---

## 5. Airflow DAG — End-to-End Orchestration

```python
# dags/nestle_procurement_pipeline.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import requests

SNOWFLAKE_CONN_ID = "snowflake_nestle"
DBT_PROJECT_DIR = "/opt/airflow/dbt/nestle_procurement_dbt"

BRONZE_LOAD_SQL = """
COPY INTO NESTLE_PROCUREMENT_DB.RAW.purchase_orders
  FROM @NESTLE_PROCUREMENT_DB.RAW.S3_PROCUREMENT_STAGE/procurement/purchase_orders/
  FILE_FORMAT = (FORMAT_NAME = 'CSV_STANDARD');
-- (repeat COPY INTO for each of the other 9 Bronze tables)
"""

def refresh_power_bi_dataset(**context):
    workspace_id = "<POWER_BI_WORKSPACE_ID>"
    dataset_id = "<POWER_BI_DATASET_ID>"
    access_token = "<OAUTH_TOKEN>"  # retrieved via Airflow Connection / secrets backend
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    resp = requests.post(url, headers={"Authorization": f"Bearer {access_token}"})
    resp.raise_for_status()

with DAG(
    dag_id="nestle_procurement_pipeline",
    description="S3 -> Snowflake Bronze -> dbt Silver/Gold -> dbt test -> Power BI refresh",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nestle", "procurement", "training"],
) as dag:

    wait_for_files = S3KeySensor(
        task_id="wait_for_purchase_orders_file",
        bucket_name="nestle-procurement-raw",
        bucket_key="procurement/purchase_orders/dt={{ ds }}/purchase_orders.csv",
        aws_conn_id="aws_nestle",
        timeout=60 * 30,
        poke_interval=60,
    )

    load_bronze = SnowflakeOperator(
        task_id="load_bronze_layer",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=BRONZE_LOAD_SQL,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    refresh_powerbi = PythonOperator(
        task_id="refresh_power_bi_dataset",
        python_callable=refresh_power_bi_dataset,
    )

    wait_for_files >> load_bronze >> dbt_run_staging >> dbt_run_marts >> dbt_test >> refresh_powerbi
```

---

## 6. Power BI — Sample DAX Measures

```dax
-- Total Spend (from Fct Purchase Order)
Total Spend =
SUM ( fct_purchase_order[line_amount] )

-- Invoice Match Rate
Invoice Match Rate =
DIVIDE (
    CALCULATE ( COUNTROWS ( fct_invoice ), fct_invoice[match_status] = "Matched" ),
    COUNTROWS ( fct_invoice )
)

-- OTIF %
OTIF % =
DIVIDE (
    CALCULATE ( COUNTROWS ( fct_shipment ), fct_shipment[otif_flag] = TRUE ),
    COUNTROWS ( fct_shipment )
)

-- Days Payable Outstanding (DPO), average
Avg Days To Pay =
AVERAGE ( fct_payment[days_to_pay] )

-- Early Payment Discount Capture Rate
Early Discount Capture Rate =
DIVIDE (
    CALCULATE ( COUNTROWS ( fct_payment ), fct_payment[early_payment_discount_applied] = TRUE ),
    COUNTROWS ( fct_payment )
)

-- Supplier Overall Score (weighted)
Supplier Composite Score =
AVERAGEX (
    fct_supplier_rating,
    fct_supplier_rating[on_time_delivery_pct] * 0.5
        + ( 100 - fct_supplier_rating[quality_rejection_pct] * 10 ) * 0.3
        + fct_supplier_rating[responsiveness_score] * 20 * 0.2
)
```

---

## 7. Quick Command Cheat-Sheet

| Task | Command |
|---|---|
| List files Snowflake can see in S3 stage | `LIST @S3_PROCUREMENT_STAGE;` |
| Load one Bronze table | `COPY INTO <table> FROM @stage/<prefix>/ FILE_FORMAT=(FORMAT_NAME='CSV_STANDARD');` |
| Run all dbt staging models | `dbt run --select staging` |
| Run all dbt mart models | `dbt run --select marts` |
| Run dbt tests | `dbt test` |
| Generate dbt lineage docs | `dbt docs generate && dbt docs serve` |
| Trigger Airflow DAG manually | `airflow dags trigger nestle_procurement_pipeline` |
| Refresh Power BI dataset via API | `POST https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes` |
