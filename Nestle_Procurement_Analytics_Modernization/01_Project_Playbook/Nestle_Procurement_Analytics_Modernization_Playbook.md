# Nestle Procurement Analytics Modernization — Master Project Playbook

**Training Program:** Weekend Data Engineering & Cloud Computing Batch
**Project Track:** Data Engineering — Cloud Data Platform (Medallion Architecture)
**Client (Fictional Training Scenario):** Nestle S.A. — Procurement Function
**Document Type:** Consolidated Business Requirement + Solution Design Playbook
**Version:** 1.0

> **Dataset & Scenario Disclaimer:** Nestle is used as a realistic, well-known reference brand purely to give this training project an authentic business context. All supplier names, plant names, purchase orders, invoices, payments, shipments and figures in this project are **100% synthetic/dummy data**, generated for teaching purposes only. Nothing in this project reflects Nestle's actual systems, suppliers, contracts or financial data.

---

## 1. Purpose of This Document

This playbook is the single reference document for the batch. Read this end-to-end before touching any code. It explains:

- Who the client is and what business problem we are solving
- What the consulting engagement is scoped to deliver
- The full solution architecture, layer by layer
- The data model (facts and dimensions) candidates must build
- How the tech stack pieces (S3, Snowflake, dbt, Airflow, Power BI) fit together
- A milestone plan candidates will follow across the weekend batch

Two companion documents sit alongside this one in the repo:

| Document | Purpose |
|---|---|
| `02_Reference_Guide/Tech_Stack_and_Integration_Reference_Guide.md` | Deep-dive primer on each tool and how they connect to each other |
| `03_Code_Reference/Code_Reference_Guide.md` | Consolidated, ready-to-adapt code: Snowflake DDL, dbt models, Airflow DAG, Power BI DAX |

Sample data lives in `04_Data/csv` and `04_Data/json`.

---

## 2. Client Background

**Nestle** is a global food and beverage company operating across categories such as dairy, confectionery/cocoa, coffee, nutrition and bottled water, manufactured and distributed through plants and distribution centers across EMEA, Americas, APAC and Africa.

Nestle's **Procurement function** is responsible for:

- Sourcing raw materials (cocoa, coffee, dairy, sugar, flavors/additives), packaging, contract co-packing, logistics services and MRO (maintenance, repair, operations) spares
- Managing a global, multi-region supplier base with varying onboarding, compliance and sustainability requirements
- Running the full **Procure-to-Pay (P2P)** cycle: Purchase Order → Goods Receipt/Shipment → Invoice → Payment
- Tracking inbound shipments from supplier origin to manufacturing plant, including cold-chain requirements for perishable raw materials (dairy, cocoa, coffee)

## 3. Business Problem / Pain Points

Nestle's procurement data today is fragmented across multiple regional ERP instances, supplier portals, freight-forwarder systems and spreadsheets. Leadership cannot get a single, trustworthy view of procurement performance. Specific pain points raised by the client:

1. **Fragmented supplier data** — Supplier master data, onboarding/compliance status, and performance ratings live in different systems with no unified "Supplier 360" view.
2. **Slow, error-prone 3-way matching** — Matching Purchase Order → Goods Receipt/Shipment → Invoice is largely manual, causing payment delays, duplicate payments and disputes.
3. **No visibility into supplier performance** — On-time delivery %, quality rejection %, and responsiveness are tracked in disconnected spreadsheets, making supplier scorecarding inconsistent across regions.
4. **Poor shipment visibility** — Inbound shipments (ocean, air, road, rail) are tracked in separate freight-forwarder/TMS portals; delays and cold-chain excursions (temperature-sensitive dairy/cocoa/coffee) are discovered too late.
5. **Cash flow / payment blind spots** — No consolidated view of payment terms, overdue invoices, early-payment discount capture, or Days Payable Outstanding (DPO) across suppliers and regions.
6. **Slow supplier onboarding** — Compliance checks (e.g. food-safety certifications, sustainability scoring, bank verification) are tracked manually, delaying time-to-first-PO for new suppliers.
7. **No self-service analytics** — Category managers depend on IT/finance to produce ad hoc Excel reports; there is no governed, reusable semantic layer for dashboards.

## 4. Project: Procurement Analytics Modernization

Nestle has engaged our consulting firm to **modernize the procurement data & analytics stack** — this is the project the candidates are staffed on.

### 4.1 Objective

Build a cloud-native, medallion-architecture data platform that ingests procurement source data (supplier, purchase order, invoice, payment, shipment, rating data), transforms it into a governed analytical model, and exposes it through Power BI so Nestle's procurement leadership can answer questions such as:

- Which suppliers are consistently late or have high quality rejection rates?
- What is our total spend by category, region, and supplier over time?
- What % of shipments are On-Time-In-Full (OTIF)? Which lanes/carriers underperform?
- What is our invoice match rate, and where are 3-way match exceptions concentrated?
- What is our Days Payable Outstanding (DPO), and are we capturing early-payment discounts?
- Which new suppliers are stuck in onboarding, and why?

### 4.2 Success Metrics (KPIs the platform must support)

| KPI | Business Question |
|---|---|
| OTIF % (On-Time-In-Full) | Are shipments arriving on time and complete? |
| Invoice Match Rate | % of invoices matching PO on qty/price without exception |
| Days Payable Outstanding (DPO) | How long, on average, do we take to pay suppliers? |
| Total Spend by Category / Region / Supplier | Where is procurement spend concentrated? |
| Supplier Scorecard (OTD %, Quality Rejection %, Rating) | Which suppliers are top/bottom performers? |
| Onboarding Cycle Time | How long does supplier onboarding take, by stage? |
| Early Payment Discount Capture Rate | Are we capturing available discounts? |

### 4.3 Scope

**In scope**

- Ingesting flat-file (CSV) and semi-structured (JSON) procurement extracts from S3 into Snowflake
- Building Bronze (raw), Silver (cleansed/conformed) and Gold (facts & dimensions) layers in Snowflake using dbt
- Modeling a star schema covering Supplier, Material, Plant, Date and Currency dimensions, and Purchase Order, Invoice, Payment, Shipment and Supplier Rating facts
- Orchestrating the end-to-end pipeline with Apache Airflow
- Exposing a Power BI-ready semantic layer (Gold-layer views) and sample dashboards/measures

**Out of scope (owned by other teams, mentioned for context only)**

- The **source-to-S3 extraction/API layer** — a separate "API/Integration team" is responsible for pulling data out of Nestle's source systems (SAP ERP, Ariba/SRM, freight-forwarder TMS, Quality Management System) and landing flat files into S3. Candidates receive these files *as-is*, already in S3-ready CSV/JSON format.
- Source system remediation / master data cleanup at the ERP level
- Building the actual AWS account/networking (candidates will simulate this conceptually — see Reference Guide)

### 4.4 Source Systems Simulated by Our Sample Data

| Source System (real-world) | Data Domain | Sample File(s) |
|---|---|---|
| Ariba / Supplier Relationship Management (SRM) | Supplier master, onboarding | `supplier_master.csv`, `supplier_onboarding.csv` |
| Quality Management System | Supplier performance ratings | `supplier_rating.csv` |
| SAP ERP — Materials Management (MM) | Material/product master, plant master | `product_master.csv`, `plant_master.csv` |
| SAP ERP — Purchasing | Purchase orders | `purchase_orders.csv`, `purchase_order_lines_nested.json` |
| SAP ERP — Accounts Payable | Invoices, payments | `invoices.csv`, `payments.csv` |
| Freight Forwarder / TMS Portal | Shipment tracking & milestones | `shipment_tracking.csv`, `shipment_tracking_events.json` |
| Treasury / Finance | Currency exchange rates | `currency_exchange_rate.csv` |

The **API/Integration team** (not part of this candidate exercise) is assumed to have already extracted these into flat files and dropped them into an S3 raw landing bucket, partitioned by source and date. Candidates start from that point.

---

## 5. Solution Architecture — Medallion Design

```
 Source Systems (simulated)          Ingestion / Landing        Snowflake — Medallion Layers                     Consumption
 ─────────────────────────           ──────────────────         ─────────────────────────────                    ───────────
 SAP ERP (PO, Invoice, Payment)  \                          BRONZE (RAW)      SILVER (STAGING)      GOLD (MART)
 Ariba/SRM (Supplier)             \                         ─────────────     ─────────────────     ─────────────
 QMS (Supplier Rating)             > API/Integration Team    Raw tables,       Cleansed, typed,       Facts & Dims
 TMS/Freight Portal (Shipment)    /   extracts to S3   -->   1:1 with source   deduped, conformed  -->  (Star Schema)  --> Power BI
 Treasury (FX Rates)             /   (CSV + JSON)            files (COPY INTO  keys, business rules      Semantic Views      Semantic Model
                                                              / Snowpipe)       (dbt staging models)   (dbt marts)         + Dashboards
                                                                                                          ^
                                                                                              Orchestrated end-to-end by Apache Airflow
```

### 5.1 Layer-by-Layer Design

**Bronze / RAW layer (Snowflake schema: `RAW`)**
- One table per source file, schema-on-read where useful (JSON loaded into a `VARIANT` column)
- Loaded via `COPY INTO` from an S3 external stage (batch) — Snowpipe is introduced conceptually for near-real-time loading
- No business logic applied; append-only or full-refresh depending on file
- 1:1 with `04_Data/csv` and `04_Data/json` sample files

**Silver / STAGING layer (Snowflake schema: `STAGING`, built as dbt staging models)**
- One `stg_` model per Bronze table
- Responsibilities: type casting, column renaming to business-friendly names, deduplication, basic null/range validation, flattening JSON (`FLATTEN` on the two JSON sources), currency standardization prep
- This is where `purchase_order_lines_nested.json` and `shipment_tracking_events.json` get flattened into row-level structures comparable to their CSV counterparts

**Gold / MART layer (Snowflake schema: `MART`, built as dbt mart models)**
- Conformed **dimensions**: `dim_supplier`, `dim_material`, `dim_plant`, `dim_date`, `dim_currency`
- Business **facts**: `fct_purchase_order`, `fct_invoice`, `fct_payment`, `fct_shipment`, `fct_shipment_event`, `fct_supplier_rating`
- Grain, keys and surrogate keys are defined explicitly (see Section 6)
- dbt tests enforce not-null, uniqueness, referential integrity (`relationships`) and accepted-value checks at this layer

**Semantic / Consumption layer**
- A small number of **Snowflake views on top of MART**, renamed and filtered specifically for Power BI consumption (e.g. `vw_supplier_scorecard`, `vw_procure_to_pay`, `vw_otif_performance`)
- Power BI connects to these views, builds a star-schema semantic model, and layers on DAX measures for the KPIs in Section 4.2

### 5.2 Orchestration

Apache Airflow owns the end-to-end schedule:

1. Sense/confirm new files have landed in the S3 raw zone (or run on a fixed schedule for training purposes)
2. Run `COPY INTO` (or trigger Snowpipe) to load Bronze tables
3. Trigger `dbt run` for staging (Silver) models
4. Trigger `dbt run` for mart (Gold) models
5. Run `dbt test` — fail the DAG loudly if data quality checks fail
6. Trigger a Power BI dataset refresh (via Power BI REST API) so dashboards reflect the latest Gold data

See the Reference Guide and Code Reference Guide for the DAG design and sample code.

---

## 6. Data Model — Facts & Dimensions

### 6.1 Dimensions

| Dimension | Grain | Key Attributes | Source File |
|---|---|---|---|
| `DIM_SUPPLIER` | 1 row per supplier | Category, country, region, currency, payment terms, risk tier, status | `supplier_master.csv` (enriched with `supplier_onboarding.csv`) |
| `DIM_MATERIAL` | 1 row per material/SKU | Category, UOM, standard cost, shelf life, hazardous flag | `product_master.csv` |
| `DIM_PLANT` | 1 row per plant/DC | Plant type, country, region | `plant_master.csv` |
| `DIM_DATE` | 1 row per calendar day | Day/week/month/quarter/year, fiscal attributes | Generated in dbt (no source file) |
| `DIM_CURRENCY` | 1 row per currency per month | Currency code, rate to USD | `currency_exchange_rate.csv` |

### 6.2 Facts

| Fact | Grain | Key Measures | Source File(s) |
|---|---|---|---|
| `FCT_PURCHASE_ORDER` | 1 row per PO line | Order qty, unit price, line amount | `purchase_orders.csv` (+ `purchase_order_lines_nested.json` for the semi-structured lesson) |
| `FCT_INVOICE` | 1 row per invoice line | Invoiced qty, unit price, line/tax/total amount, match status | `invoices.csv` |
| `FCT_PAYMENT` | 1 row per payment | Payment amount, days to pay, discount flag | `payments.csv` |
| `FCT_SHIPMENT` | 1 row per shipment | Ship/estimated/actual arrival dates, OTIF flag | `shipment_tracking.csv` |
| `FCT_SHIPMENT_EVENT` | 1 row per tracking checkpoint event | Event status, temperature reading | `shipment_tracking_events.json` (flattened) |
| `FCT_SUPPLIER_RATING` | 1 row per supplier per rating period | OTD %, quality rejection %, responsiveness, overall rating | `supplier_rating.csv` |

### 6.3 Conceptual Relationships

```
DIM_SUPPLIER ──┬── FCT_PURCHASE_ORDER ──── FCT_INVOICE ──── FCT_PAYMENT
               ├── FCT_SHIPMENT ──── FCT_SHIPMENT_EVENT
               └── FCT_SUPPLIER_RATING

DIM_MATERIAL ──── FCT_PURCHASE_ORDER
DIM_PLANT ──┬── FCT_PURCHASE_ORDER
            └── FCT_SHIPMENT (destination)
DIM_DATE ──── (every fact, via its primary date)
DIM_CURRENCY ──── FCT_PURCHASE_ORDER / FCT_INVOICE / FCT_PAYMENT
```

`PO_ID` links `FCT_PURCHASE_ORDER` → `FCT_INVOICE` → `FCT_SHIPMENT`. `INVOICE_ID` links `FCT_INVOICE` → `FCT_PAYMENT`. Note: not every PO has an invoice yet (still open), and not every invoice has a payment yet (still pending/disputed) — this mirrors real procurement funnels and is intentional for teaching data-quality/completeness checks.

---

## 7. Tech Stack Summary

| Layer | Tool | Role in This Project |
|---|---|---|
| Data Lake | **AWS S3** | Landing zone for raw CSV/JSON extracts, partitioned by source/date; acts as the durable, cheap storage layer before Snowflake ingestion |
| Cloud Data Platform | **Snowflake** | Hosts Bronze/Silver/Gold schemas; compute via virtual warehouses; loads data via external stage + `COPY INTO`/Snowpipe; handles semi-structured JSON via `VARIANT`/`FLATTEN` |
| Transformation | **dbt (data build tool)** | Converts Bronze → Silver → Gold using SQL models, enforces data quality via tests, documents lineage |
| Orchestration | **Apache Airflow** | Schedules and sequences ingestion → transformation → testing → BI refresh as a single DAG |
| Visualization | **Power BI** | Connects to Gold/semantic views in Snowflake; builds star-schema semantic model and dashboards for procurement KPIs |

Full explanations, connection steps and command references are in `02_Reference_Guide/Tech_Stack_and_Integration_Reference_Guide.md`.

---

## 8. Training Batch Milestone Plan

| Milestone | Candidates Deliver |
|---|---|
| **M1 — Foundations** | Understand this playbook; set up AWS S3 bucket structure; upload sample CSV/JSON to raw zone |
| **M2 — Bronze Layer** | Create Snowflake storage integration + external stage; create Bronze tables; load via `COPY INTO` |
| **M3 — Silver Layer (dbt)** | Build dbt staging models for all sources, including `FLATTEN` on the two JSON files |
| **M4 — Gold Layer (dbt)** | Build dimension and fact models (star schema); add dbt tests |
| **M5 — Orchestration** | Build an Airflow DAG chaining ingestion → dbt run → dbt test |
| **M6 — Semantic Layer & Power BI** | Build semantic views; connect Power BI; build supplier scorecard + OTIF + spend dashboards |
| **M7 — Showcase** | Present end-to-end pipeline and dashboard to the batch |

## 9. Deliverables Checklist for Candidates

- [ ] S3 bucket with raw/bronze zone structure
- [ ] Snowflake databases/schemas for RAW, STAGING, MART
- [ ] Storage integration + external stage + file formats (CSV and JSON)
- [ ] Bronze tables loaded for all 10 CSV files + 2 JSON files
- [ ] dbt project with staging models for every Bronze table
- [ ] dbt mart models for all 5 dimensions and 6 facts listed in Section 6
- [ ] dbt tests (`unique`, `not_null`, `relationships`, `accepted_values`) on key models
- [ ] Airflow DAG orchestrating the full pipeline
- [ ] Power BI report with at least: Supplier Scorecard, OTIF Performance, Spend Analysis, Payment/DPO view

## 10. Glossary

| Term | Meaning |
|---|---|
| **OTIF** | On-Time-In-Full — shipment arrived by the estimated date and with the full ordered quantity |
| **3-way match** | Matching PO, Goods Receipt/Shipment, and Invoice before approving payment |
| **DPO** | Days Payable Outstanding — average number of days taken to pay suppliers |
| **Medallion architecture** | Bronze (raw) → Silver (cleansed/conformed) → Gold (business-ready facts/dims) layering pattern |
| **SCD** | Slowly Changing Dimension — pattern for tracking historical changes to dimension attributes |
| **Semantic layer** | Business-friendly, BI-ready views/model sitting on top of the Gold layer |
