# Data Engineering Interview Q&A — Snowflake, dbt, AWS S3 & SQL

**Level:** 3–5 years hands-on data engineering
**Scope:** Snowflake architecture & features, dbt, AWS S3, SQL scenarios, data modeling, SCD, incremental/CDC patterns, and Snowflake Cortex / AI-in-the-pipeline.

A note on how to use this: the goal in a technical interview is to demonstrate you understand the *concept*, the *tradeoffs*, and can reason about *when to use what* — not to recite a script. Every answer below is written as a general, defensible explanation with a concrete example. Where an example is domain-flavored, it uses a generic procurement/retail data warehouse scenario (the same kind of domain this practice project models) purely to make the idea concrete — treat these as illustrations of the concept, not a transcript to memorize word-for-word. If asked "tell me about a project where you did X," answer in your own words, from what you actually built and understand.

---

## Table of Contents
1. [Snowflake Architecture & Core Concepts](#1-snowflake-architecture--core-concepts)
2. [Data Loading — COPY INTO, File Formats, Error Handling & Snowpipe](#2-data-loading--copy-into-file-formats-error-handling--snowpipe)
3. [Views, Secure Views, Materialized Views & Dynamic Tables](#3-views-secure-views-materialized-views--dynamic-tables)
4. [Stored Procedures & Exception Handling](#4-stored-procedures--exception-handling)
5. [Snowflake Cortex & AI in the Data Pipeline](#5-snowflake-cortex--ai-in-the-data-pipeline)
6. [Data Modeling — Star Schema, Facts & Dimensions](#6-data-modeling--star-schema-facts--dimensions)
7. [Slowly Changing Dimensions (SCD Types)](#7-slowly-changing-dimensions-scd-types)
8. [Incremental Loading, CDC & Late-Arriving Data](#8-incremental-loading-cdc--late-arriving-data)
9. [dbt — Concepts, Testing, Incremental Models](#9-dbt--concepts-testing-incremental-models)
10. [SQL Scenarios — CTEs, Subqueries, Joins, Window Functions](#10-sql-scenarios--ctes-subqueries-joins-window-functions)
11. [AWS S3 Fundamentals for Data Engineering](#11-aws-s3-fundamentals-for-data-engineering)
12. [Query Debugging & Performance Tuning](#12-query-debugging--performance-tuning)

---

## 1. Snowflake Architecture & Core Concepts

**Q1. Describe Snowflake's architecture.**
Snowflake uses a multi-cluster shared data architecture with three separated layers: **storage** (centralized, compressed columnar data, effectively backed by cloud object storage), **compute** (virtual warehouses — independent MPP clusters that read the same storage), and **cloud services** (query optimization, metadata, security, transactions). Because storage and compute are decoupled, you can scale either independently.
*Analogy:* Think of a shared warehouse building (storage) with multiple independent forklift teams (compute clusters) that can all access the same shelves at once without stepping on each other.

**Q2. What is a micro-partition?**
Snowflake automatically splits table data into micro-partitions of roughly 50–500MB (uncompressed), stored in a columnar format. Each micro-partition stores metadata (min/max values, distinct counts) so the query optimizer can prune partitions that can't contain relevant data, without scanning them.
*Example:* A query filtering `WHERE order_date = '2025-01-15'` only scans partitions whose date-range metadata overlaps that day — not the whole multi-year fact table.

**Q3. What is a virtual warehouse, and how does scaling work?**
A virtual warehouse is a cluster of compute resources (T-shirt sized XS–6XL) used to run queries and DML. Warehouses can be resized (vertical scaling, more power per query) or configured as multi-cluster (horizontal scaling, more concurrent queries), and can auto-suspend/auto-resume to control cost.
*Example:* A reporting warehouse might auto-suspend after 5 minutes idle, and scale out to multiple clusters during month-end close when 50 analysts hit dashboards simultaneously.

**Q4. What is Time Travel, and how is it different from Fail-safe?**
Time Travel lets you query, clone, or restore data as it existed at a past point in time (up to 90 days, edition-dependent) using `AT`/`BEFORE` clauses or `UNDROP`. Fail-safe is a further 7-day period *after* Time Travel expires, but it's Snowflake-managed disaster recovery only — you can't query it yourself; you'd need to contact Snowflake support.
*Example:* `SELECT * FROM orders AT (OFFSET => -3600)` to see the table one hour ago, or `UNDROP TABLE orders` to recover an accidentally dropped table within the retention window.

**Q5. What is zero-copy cloning?**
`CREATE TABLE new_tbl CLONE old_tbl` creates a clone that shares the same underlying micro-partitions (metadata-only operation, near-instant, no extra storage cost initially). Storage is only consumed for the delta once either the source or the clone diverges (copy-on-write).
*Example:* Cloning a multi-terabyte production schema into a dev/QA environment in seconds, without duplicating storage cost, to test a migration safely.

**Q6. What is clustering, and when do you need a clustering key?**
Snowflake automatically clusters data as it's loaded, but for very large tables with a query pattern that doesn't align with natural load order, you can define an explicit clustering key so Snowflake reorganizes micro-partitions to keep related rows together, improving partition pruning.
*Example:* A huge fact table loaded by ingestion timestamp, but almost always queried by `region` and `product_category` — clustering on those columns can dramatically cut scan time.

**Q7. Explain Snowflake's caching layers.**
There are three: the **result cache** (24 hours, returns instantly for an identical repeated query with no underlying data change), the **local disk/SSD cache** on warehouse compute nodes (caches recently scanned micro-partitions), and **metadata cache** in the cloud services layer (statistics used for pruning and query planning).
*Example:* Running the exact same dashboard query twice in a row returns from result cache with zero compute cost the second time.

**Q8. What are Streams, and what problem do they solve?**
A Stream is a native change-data-capture object on a table — it tracks inserted, updated, and deleted rows since the last time it was consumed (using hidden metadata columns), without you needing external CDC tooling.
*Example:* A Stream on a raw `orders` staging table feeds a Task that MERGEs only the changed rows into the curated fact table, instead of reprocessing the whole table every run.

**Q9. What are Tasks, and how do they relate to Streams?**
Tasks schedule or trigger SQL/stored-procedure execution, and can be chained into a DAG. Paired with Streams, they form a lightweight native ELT pattern: Stream detects what changed, Task processes it on a schedule or when the stream has data (`WHEN SYSTEM$STREAM_HAS_DATA(...)`).
*Example:* A root task runs every 15 minutes, checks a stream for new S3-loaded rows, and only fires downstream transformation tasks if there's actually new data — avoiding wasted compute.

**Q10. What is Snowpipe, and how is it different from a scheduled COPY INTO?**
Snowpipe is serverless, continuous/near-real-time ingestion — either auto-ingest (triggered by cloud storage event notifications, e.g., S3 → SQS) or via REST API. It uses Snowflake-managed compute billed per-file/per-second, not a warehouse you manage, so latency is typically under a minute from file landing to load.
*Example:* A vendor drops a new invoice CSV in S3 every few minutes; Snowpipe loads it automatically within seconds instead of waiting for an hourly batch job.

**Q11. What is a Resource Monitor?**
A cost-governance object that tracks credit consumption for one or more warehouses against a quota, and can trigger actions (notify, suspend after current queries finish, or suspend immediately) at defined thresholds.
*Example:* Setting a monthly resource monitor at 80% "notify," 100% "suspend" on a dev warehouse to prevent a runaway query from blowing the month's compute budget.

**Q12. Why do people describe Snowflake as separating storage and compute, and why does that matter?**
Because you pay for compute only while warehouses are running, and storage independently — so you can spin up a huge warehouse for a heavy nightly load and suspend it immediately after, without over-provisioning compute permanently just to hold data. It also means multiple teams can run isolated workloads (BI vs. ELT vs. data science) against the same data without contention.
*Analogy:* Like renting extra kitchen staff only for the dinner rush, while the pantry (storage) stays exactly the same size and cost all day.

---

## 2. Data Loading — COPY INTO, File Formats, Error Handling & Snowpipe

**Q13. Walk through a basic `COPY INTO` statement.**
```sql
COPY INTO procurement.raw.purchase_orders
FROM @procurement.raw.s3_stage/orders/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*orders_.*[.]csv'
ON_ERROR = 'CONTINUE'
PURGE = TRUE;
```
It reads staged files matching a pattern, parses them per the file format, and loads rows into the target table, with control over error handling and post-load cleanup.

**Q14. What are the options for `ON_ERROR`, and when would you use each?**
- `ABORT_STATEMENT` (default): the whole load fails on the first bad row — safest for critical, small, well-controlled files.
- `CONTINUE`: skips bad rows, loads the rest — useful for large, generally-clean files where a handful of malformed rows shouldn't block the batch.
- `SKIP_FILE` / `SKIP_FILE_<n>`: skips an entire file if it has any (or more than n) errors — useful when a whole file is likely corrupt and partial-loading it would be worse than skipping it.
*Example:* For a daily vendor feed, `SKIP_FILE_10` might skip a file if more than 10 rows are malformed, on the assumption something upstream broke that file specifically.

**Q15. How do you validate a file before actually loading it?**
`VALIDATION_MODE = RETURN_ERRORS` (or `RETURN_ALL_ERRORS` / `RETURN_<n>_ROWS`) runs the COPY as a dry run and returns the errors it *would* hit, without loading anything — good for a pre-flight check in a pipeline before committing to a load.

**Q16. There's no single "error table" object in Snowflake — so how do teams actually implement an error/quarantine table pattern?**
Common pattern: load raw data permissively (e.g., as VARIANT/semi-structured, or with wide/loose typing) into a landing table first, so nothing is rejected at ingestion. Then run validation logic (data quality checks, type casts, referential checks) in a downstream transformation step, and route rows that fail validation into a dedicated `*_rejects`/`*_errors` table with the failure reason, instead of silently dropping them or letting `ON_ERROR` discard them invisibly.
*Example:* A `stg_purchase_orders_rejects` table capturing rows with a null `po_number` or an amount that fails a numeric cast, alongside the reason, for a data steward to review — rather than those rows vanishing during COPY.

**Q17. How do you check the history/status of past loads?**
`INFORMATION_SCHEMA.COPY_HISTORY` (or the `COPY_HISTORY` table function) and the account's `LOAD_HISTORY` show what was loaded, when, row counts, and errors — useful for auditing and for building load-monitoring dashboards.

**Q18. What does `MATCH_BY_COLUMN_NAME` do, and when is it useful?**
It lets COPY INTO match staged file columns (CSV with a header, or JSON keys) to target table columns by name instead of strict positional order — handy when source files occasionally add/reorder columns and you don't want the pipeline to silently misalign data.

**Q19. Why prefer Parquet over CSV for large loads where you have a choice?**
Parquet is columnar and compressed, so Snowflake (and any columnar engine) can skip irrelevant columns and get better compression ratios, meaning faster loads and less storage/network overhead versus row-based, uncompressed CSV.

**Q20. What's the difference between Snowpipe and Snowpipe Streaming?**
Classic Snowpipe ingests *files* (batched, even if small/frequent). Snowpipe Streaming ingests rows directly via an API without staging files first, further reducing latency for true row-by-row/streaming sources (e.g., a Kafka connector) instead of a create-file-then-notify pattern.

**Q21. How would you design ingestion so a single bad file doesn't block the whole pipeline?**
Isolate load failures per file/batch (e.g., `SKIP_FILE` behavior, or wrapping loads in per-file try/catch in a procedure), log failures to a monitoring table, and alert — so one malformed vendor file delays only its own data, not every other source feeding the same warehouse.

**Q22. What's `PURGE` in COPY INTO, and what's a risk with using it carelessly?**
`PURGE = TRUE` deletes the staged file after a successful load, keeping the stage clean. The risk: if you ever need to reprocess/replay that data (bug found downstream, schema fix needed), the source file is gone — many teams instead move processed files to an "archive" prefix rather than deleting, preserving replay capability.

---

## 3. Views, Secure Views, Materialized Views & Dynamic Tables

**Q23. What is a standard view, and what's its main limitation?**
A view stores only the query definition; every time it's queried, Snowflake re-executes the underlying SQL. No extra storage, always reflects current base-table data — but repeated expensive queries against a heavy view cost compute every single time.

**Q24. What is a Secure View, and why would you choose it over a normal view?**
A Secure View hides its definition and underlying table structure from users who can query it but shouldn't see how it's built (important when sharing data across roles or accounts, e.g., Snowflake Data Sharing, or row-level-security patterns). Trade-off: the optimizer has less visibility into the view internals, so some query optimizations (like certain predicate pushdowns) can be more limited than on a normal view.
*Example:* Sharing a "vendor spend summary" view externally with a partner without exposing the raw contract-pricing table it's built on.

**Q25. What is a Materialized View, and what are its constraints?**
A Materialized View stores its result set physically and Snowflake automatically keeps it in sync as the base table changes (background maintenance, at extra storage + compute cost). Historically limited to a single base table with restrictions on the kinds of aggregations/joins allowed — best for a frequently-queried, expensive aggregation over one large table.
*Example:* A materialized view pre-aggregating daily spend-by-category from a huge line-item fact table, so dashboards hit the small precomputed result instead of scanning millions of rows each time.

**Q26. What is a Dynamic Table, and how is it different from a Materialized View?**
A Dynamic Table is defined by a query (which *can* include joins and multi-table transformations, unlike classic materialized views) plus a `TARGET_LAG` — Snowflake automatically figures out how to refresh it (incrementally where possible, full refresh otherwise) to stay within that freshness target. It's essentially a declarative alternative to hand-building Streams+Tasks pipelines.
*Example:* `CREATE DYNAMIC TABLE curated.fact_spend TARGET_LAG='5 minutes' WAREHOUSE=etl_wh AS SELECT ... FROM staging JOIN dims ...` — no manual stream/task wiring needed to keep it fresh.

**Q27. When would you choose Dynamic Tables over a dbt incremental model, or vice versa?**
Dynamic Tables push transformation + orchestration + incremental refresh entirely into Snowflake with minimal code — great for simpler, warehouse-native pipelines. dbt gives you version control, testing, documentation, modularity across a large multi-model DAG, and warehouse portability — better when transformation logic is complex, team-maintained, and needs software-engineering rigor (many teams now use both: dbt for the bulk of modeled transformations, Dynamic Tables for specific low-latency serving layers).

**Q28. Rank views, secure views, materialized views, and dynamic tables by "how fresh vs. how expensive."**
Standard/Secure View: always 100% fresh, full compute cost every query. Materialized View / Dynamic Table: near-fresh (configurable lag for dynamic tables), but pay maintenance cost continuously and pay storage for the result — the trade is "pay a little, continuously, to make every read cheap and fast."

**Q29. Can a secure view ever be a materialized view?**
Yes — `CREATE SECURE MATERIALIZED VIEW` is supported when you need both the performance benefit of precomputation and the security/definition-hiding of a secure view.

**Q30. Give a scenario where you'd deliberately avoid a materialized view even though a query is slow.**
If the base table changes very frequently (e.g., every few seconds) relative to how often the view is actually queried (e.g., a few times a day), the continuous maintenance cost of a materialized view/dynamic table would exceed the cost of just recomputing on the rare occasions someone queries it — a plain view (or even just running the query directly) is cheaper overall.

---

## 4. Stored Procedures & Exception Handling

**Q31. What languages can you write Snowflake stored procedures in?**
Snowflake Scripting (a procedural extension of SQL), JavaScript, Python and Java/Scala via Snowpark. Choice depends on team skillset and whether you need external libraries (Python/Snowpark) vs. simple procedural SQL control flow (Snowflake Scripting).

**Q32. What's the difference between a stored procedure and a UDF?**
A stored procedure can perform DDL/DML, control flow, and side effects, and is invoked with `CALL`; it doesn't have to return a scalar value usable inline in a SELECT. A UDF (scalar or table) is a pure function used *inside* a SQL statement to compute a value per row/set, and can't perform side effects like DDL.

**Q33. How does exception handling work in Snowflake Scripting?**
A block can include an `EXCEPTION WHEN <condition> THEN ...` section, similar to try/catch — you can catch built-in conditions (e.g., `STATEMENT_ERROR`) or user-defined exceptions raised explicitly, log the error, and decide whether to continue, retry, or re-raise.
```sql
BEGIN
  INSERT INTO fact_spend SELECT * FROM stg_spend WHERE load_id = :load_id;
EXCEPTION
  WHEN OTHER THEN
    INSERT INTO etl_error_log(proc_name, error_message, run_ts)
    VALUES ('load_fact_spend', :SQLERRM, CURRENT_TIMESTAMP());
    RAISE;
END;
```

**Q34. Why wrap ETL steps in stored procedures with exception handling instead of letting the pipeline fail silently or fully abort?**
So you can log exactly what failed and why (into an error/audit table), decide programmatically whether a failure in one step should stop the whole pipeline or just that step, and give downstream monitoring something concrete to alert on — versus a raw script that either succeeds completely or fails with no diagnostic trail.

**Q35. What's a cursor, and when would you actually need one in Snowflake Scripting?**
A cursor lets you iterate row-by-row over a result set inside a procedure — rarely needed for set-based transformations (which should stay in plain SQL for performance), but useful for genuinely row-by-row administrative logic, e.g., dynamically generating and executing a DDL statement per table in a schema.

**Q36. How would you implement retry logic for a transient failure (e.g., a flaky external call from a procedure)?**
Wrap the risky operation in a loop with a counter and an EXCEPTION block that increments a retry counter and re-attempts up to N times with a short delay, only re-raising the exception once retries are exhausted — logging each attempt for observability.

**Q37. What's the risk of an overly broad `WHEN OTHER THEN` exception handler?**
It can silently swallow errors you didn't anticipate, masking real bugs (e.g., a schema drift causing a column-not-found error looks the same as an expected, ignorable warning) — best practice is to catch specific conditions where possible and always log the actual error before deciding to suppress vs. re-raise.

**Q38. Can a stored procedure call another stored procedure, and why would you structure things that way?**
Yes — decomposing a large orchestration procedure into smaller single-purpose procedures (extract-stage, validate, merge, log) makes each step independently testable and reusable, similar to functions in general-purpose programming, rather than one monolithic script.

---

## 5. Snowflake Cortex & AI in the Data Pipeline

**Q39. What is Snowflake Cortex, at a high level?**
A set of managed AI/LLM capabilities available directly as SQL functions inside Snowflake — so you can call an LLM (`SNOWFLAKE.CORTEX.COMPLETE`), summarize text, translate, classify sentiment, or search, without exporting data to an external service. Because the data never leaves Snowflake's governance boundary, it fits naturally into existing RBAC/masking controls.

**Q40. Give an example of using Cortex inside a transformation pipeline (e.g., a dbt model).**
Using `SNOWFLAKE.CORTEX.COMPLETE()` or a classification function inside a SQL/dbt model to standardize messy free-text vendor names or categorize unstructured spend descriptions into a fixed taxonomy, instead of shipping that data out to a separate ML microservice and reintegrating the result.

**Q41. What is Cortex Search, and how is it different from Cortex Analyst?**
Cortex Search is a managed semantic/retrieval service (embeddings + vector search under the hood) typically used for RAG-style lookups over unstructured or semi-structured content. Cortex Analyst is natural-language-to-SQL against a defined semantic model, aimed at letting business users ask plain-English questions of structured data.

**Q42. Where does Document AI fit into a data engineering pipeline?**
It extracts structured fields from unstructured documents (like PDFs/invoices) directly, which is useful upstream of a typical structured ELT flow — e.g., pulling PO number, vendor, and line-item amounts out of scanned invoices before they ever hit a normal COPY INTO / dbt pipeline.

**Q43. What are the governance advantages of doing AI inference inside the warehouse versus calling an external API from your pipeline?**
Data doesn't leave the platform's existing security perimeter (RBAC, masking, data residency all still apply), there's one less external dependency/credential to manage, and lineage/auditability stay inside the same system you already govern — versus an external call which introduces a new data-egress and compliance surface to review.

**Q44. What's a reasonable caution to raise about using an LLM function inside a deterministic ELT pipeline?**
LLM output isn't guaranteed to be perfectly deterministic or 100% accurate (e.g., a classification or extraction call can occasionally misfire), so it needs the same validation/testing discipline as any other transformation — don't treat its output as ground truth without spot-checks, especially for anything feeding financial or compliance-sensitive reporting.

**Q45. Have you used AI coding assistants (e.g., Claude Code, Copilot) in your day-to-day data engineering work? What for?**
Yes — practical, common uses include: scaffolding boilerplate dbt models/tests faster, explaining an unfamiliar error message or query plan, drafting first-pass documentation, and reviewing SQL for obvious issues before a PR — it speeds up the mechanical parts of the job, but the engineer still owns reviewing and validating the logic, exactly like reviewing a colleague's PR.

**Q46. What's the difference between Cortex's built-in functions and building your own ML model in Snowpark?**
Cortex functions are managed, pre-trained, general-purpose (summarize/translate/classify/complete) — no training required, fast to adopt. Snowpark ML lets you train and deploy a custom model on your own data when you need something purpose-built and more accurate for a specific business problem than a general LLM function would give you.

---

## 6. Data Modeling — Star Schema, Facts & Dimensions

**Q47. What is a star schema, and how is it different from a snowflake schema?**
A star schema has a central fact table linked directly to denormalized dimension tables (one hop away, wide dimensions). A snowflake schema normalizes dimensions further into sub-tables (e.g., splitting `dim_product` into `dim_product` + `dim_category`), reducing redundancy but requiring more joins to query.

**Q48. What defines the "grain" of a fact table, and why does it matter so much?**
The grain is the precise definition of what one row represents (e.g., "one row per purchase order line, per day"). Getting the grain wrong — or mixing grains in one fact table — causes double-counting or incorrect aggregation, so it should be defined explicitly and enforced (e.g., via a uniqueness test on the natural key) before any measures are added.

**Q49. What's the difference between additive, semi-additive, and non-additive facts?**
Additive facts can be summed across every dimension (e.g., spend amount — sum across time, vendor, category, all valid). Semi-additive facts can be summed across some dimensions but not others, classically not across time (e.g., an account balance — summing balances across days doesn't mean anything, but summing across accounts at a single point in time does). Non-additive facts can't be meaningfully summed at all (e.g., a unit price or a percentage — you'd average or recompute, not sum).

**Q50. Why do dimension tables use surrogate keys instead of the source system's natural/business key?**
Surrogate keys decouple the warehouse from source-system key changes/reuse, and are essential for supporting SCD Type 2 (the same business entity needs multiple surrogate-keyed rows over time, but only one natural key).

**Q51. What is a conformed dimension?**
A dimension built once and reused consistently across multiple fact tables/subject areas, so that, e.g., "Vendor" or "Date" means exactly the same thing whether you're looking at procurement spend or finance AP data — enabling cross-fact-table (drill-across) analysis without redefinition.

**Q52. What's a role-playing dimension? Give an example.**
The same physical dimension used in different logical roles within one fact table. A single `dim_date` table can be joined three times to a fact table as Order Date, Ship Date, and Delivery Date (typically via views/aliases), rather than maintaining three separate near-identical date dimensions.

**Q53. What's a junk dimension?**
A dimension that groups several unrelated, low-cardinality flags/indicators (e.g., rush-order flag, approval status, payment-terms code) into a single table, avoiding a fact table cluttered with many tiny separate dimension keys.

**Q54. Why might you choose a star schema over a fully normalized (3NF) model for an analytics warehouse?**
Fewer joins for typical BI queries, simpler and more predictable for query optimizers and end-user tools (and end users) to reason about, at the cost of some redundancy — the classic OLAP (read-optimized, denormalized) vs. OLTP (write-optimized, normalized) tradeoff.

**Q55. How would you model a many-to-many relationship in a star schema (e.g., one purchase order can have multiple approvers)?**
A bridge/associative table between the fact and the dimension (or between two dimensions), sometimes with a weighting factor if you need to avoid double-counting a measure when it fans out across the many-to-many join.

**Q56. What's a factless fact table?**
A fact table with no measures, just foreign keys — used to record that an event or relationship occurred (e.g., "vendor X was invited to bid on RFP Y") where the interesting fact is the relationship itself, not a number.

---

## 7. Slowly Changing Dimensions (SCD Types)

**Q57. What problem do SCDs solve in the first place?**
Source systems often only give you the *current* state of an entity (e.g., a vendor's current address or risk tier). SCD strategies decide how much history of *changes* to that entity you keep in the warehouse, and how that history interacts with facts recorded at different points in time.

**Q58. Explain SCD Type 1.**
Overwrite in place — no history retained, the dimension always reflects the latest value. Simplest to implement, but any historical fact re-joined to that dimension will now see the *current* value, which can silently rewrite history in reports (e.g., re-categorizing all historical spend under a vendor's new category, even spend recorded before the change).

**Q59. Explain SCD Type 2.**
Insert a new row for every change, with a new surrogate key, effective/expiry dates (or valid_from/valid_to), and usually a current-flag. Historical facts keep pointing to the surrogate key that was current at the time, so history stays accurate as-of-that-point-in-time.
*Example:* A vendor changes risk tier from "Standard" to "High Risk" — Type 2 adds a new row with a new surrogate key and today's date as `valid_from`; every purchase order before that date still joins to the old row showing "Standard."

**Q60. Explain SCD Type 3.**
Adds a limited number of extra columns (e.g., `previous_value`, `current_value`) to track only the last change, not a full history — used when you only ever need "what was it before the most recent change," not arbitrary point-in-time history.

**Q61. Explain SCD Type 4 (and when it's preferred over Type 2 on the same table).**
Splits current attributes into a "current" dimension table and moves historical versions into a separate history table. Useful when the dimension changes very frequently and you don't want every query against the "current" table to wade through a large volume of historical rows just to get the latest state.

**Q62. What is SCD Type 6, and why the name "6"?**
A hybrid combining Type 1 + 2 + 3 (1+2+3=6, informally): you keep full history like Type 2 (new row per change), but also carry a "current value" column on every historical row (Type 1/3 style), so a report joining to an old historical fact row can *either* see the value as it was back then *or* the value as of today, depending on which column it reads.

**Q63. How do you implement SCD Type 2 in dbt without hand-writing all the surrogate-key/date logic yourself?**
dbt **snapshots** — you point a snapshot at a source/model, choose a `check` (compares specified columns) or `timestamp` strategy, and dbt automatically manages `dbt_valid_from`, `dbt_valid_to`, and a scd surrogate key for you on each run.

**Q64. A dimension's Type 2 history got corrected retroactively upstream (e.g., the source system fixed a data-entry error from 3 months ago). Do you also retroactively fix your Type 2 history?**
Generally, a genuine data-entry *correction* should be fixed retroactively (it was never a real change, just wrong data) — whereas a genuine business change should never be rewritten (that's real history). The key interview point: distinguish "this was always wrong" (fix it) from "this used to be true and then changed" (preserve it).

---

## 8. Incremental Loading, CDC & Late-Arriving Data

**Q65. Full load vs. incremental load — what's the tradeoff?**
Full load (truncate + reload everything) is simple and self-healing (no drift risk) but expensive and slow as data grows. Incremental load only processes new/changed data — fast and cheap at scale, but more complex, and any bug in the incremental logic can silently accumulate drift over time if not caught.

**Q66. What are the main ways to detect "what changed" for an incremental load?**
Timestamp/watermark columns (e.g., `updated_at > last_watermark`), log-based CDC (reading the source database's transaction log, e.g., via Debezium/Fivetran), trigger-based capture in the source system, or Snowflake Streams for changes already inside Snowflake.

**Q67. What's the main weakness of watermark-based (`updated_at`) incremental loading?**
It depends entirely on the source reliably stamping every insert/update with an accurate `updated_at` — a hard delete produces no row to detect at all (you need a separate mechanism, like a periodic full reconciliation or CDC that captures deletes explicitly), and clock skew/backfills can cause silently missed rows.

**Q68. Walk through a MERGE-based incremental (upsert) pattern.**
```sql
MERGE INTO curated.dim_vendor tgt
USING staging.stg_vendor_changes src
  ON tgt.vendor_id = src.vendor_id
WHEN MATCHED THEN UPDATE SET tgt.vendor_name = src.vendor_name, tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN INSERT (vendor_id, vendor_name, updated_at)
  VALUES (src.vendor_id, src.vendor_name, src.updated_at);
```
Matched rows update in place, unmatched rows insert — a single idempotent statement that's safe to re-run.

**Q69. Why does idempotency matter so much in incremental pipelines?**
Pipelines fail and get re-run — an idempotent step (like MERGE keyed on a natural/business key) produces the same end state whether it runs once or is accidentally re-triggered twice on the same batch, versus a naive `INSERT` which would duplicate rows on a re-run.

**Q70. What is a late-arriving fact, and how do you handle it?**
A fact record that arrives and needs to reference a dimension member that doesn't exist in the dimension table yet (e.g., an invoice line references a vendor not yet loaded into `dim_vendor`). Standard handling: insert an "inferred member" row into the dimension with just the natural key and placeholder/unknown attributes, so the fact can still join correctly; when the real dimension data arrives later, update that inferred row (Type 1 style) in place.

**Q71. What is a late-arriving dimension update, and how should it interact with Type 2 history?**
This is when a *change* to a dimension attribute is reported later than when it actually happened (e.g., you're told today that a vendor's tier changed two weeks ago). If your Type 2 history is date-driven, you may need to insert the new row with a `valid_from` matching the real effective date (not today), and adjust any fact joins that should now point to it — trickier than a same-day change, and worth calling out explicitly as a known edge case in the design.

**Q72. What's the general problem of "out-of-order" events in a streaming/CDC context, and how is it typically handled?**
Events don't always arrive in the order they occurred (network delay, retries, batching). Streaming systems typically use watermarks — an assertion like "we don't expect events older than X to still be arriving" — plus an allowed-lateness window, after which late data either gets dropped, sent to a separate late-arrival path, or triggers a correction/re-aggregation.

**Q73. How do you detect that an incremental pipeline has drifted from the source (i.e., "silent" data loss)?**
Periodic reconciliation — compare row counts and/or checksums/aggregates (e.g., total spend) between source and warehouse on a schedule, and alert on divergence beyond a tolerance, rather than assuming the incremental logic is perfect forever.

**Q74. Deletes are notoriously the hardest part of incremental/CDC pipelines — why?**
Most incremental patterns detect *changes to existing rows* via a timestamp, but a hard delete leaves nothing behind to detect via a watermark — you either need log-based CDC (which explicitly captures deletes), a soft-delete convention at the source, or a periodic full-key reconciliation to catch rows that disappeared.

**Q75. What's a "snapshot" reconciliation pattern for catching missed deletes?**
Periodically compare the full set of natural keys currently in the source against what's in the warehouse; keys present in the warehouse but missing from the source get marked deleted/expired (Type 2 style) even though no explicit delete event was ever captured.

**Q76. In an incremental dbt model, what does the `is_incremental()` macro actually do, and why is it needed?**
It's a compile-time conditional that's only `true` when the model already exists and is being run incrementally (not on the very first `--full-refresh` build). It's how you write one model that behaves differently — full historical scan on first build, filtered `WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})` on every run after.

---

## 9. dbt — Concepts, Testing, Incremental Models

**Q77. In your own words, why dbt?**
It brings software-engineering discipline — version control, code review, automated testing, documentation, modularity/DRY via reusable macros, and CI — to the SQL transformation layer, so "the T in ELT" isn't a pile of ad hoc scripts but a maintained, tested codebase with a single source of truth for business logic.

**Q78. What are dbt materializations, and when would you use each?**
- **view**: no storage, always fresh, cheap to build, costly to query if expensive/frequent.
- **table**: fully rebuilt each run, simple and fast to query, but a full rebuild cost every time.
- **incremental**: only processes new/changed rows on subsequent runs — for large, append-or-slowly-changing datasets.
- **ephemeral**: not built as a database object at all, inlined as a CTE into whatever references it — good for lightweight, reusable logic you don't need to query directly.

**Q79. What incremental strategies does dbt support, and when would you pick `merge` vs. `append` vs. `delete+insert`?**
`append` — just inserts new rows, fastest, but only safe when you're certain there's no possibility of re-processing the same row twice (no updates, no re-runs of overlapping windows). `merge` — upsert by unique key, safest general-purpose choice when updates can happen. `delete+insert` — deletes rows matching an incoming batch's keys/partition, then inserts fresh — often used when reprocessing a whole day/partition at once is simpler than row-level matching.

**Q80. What's the difference between a generic test and a singular test in dbt?**
Generic tests (built-in: `not_null`, `unique`, `relationships`, `accepted_values`, plus custom ones from packages like `dbt_utils`) are reusable, parameterized, and applied via YAML config to any column/model. Singular tests are one-off `.sql` files asserting something specific to your business logic (the query should return zero rows if the assertion holds).
*Example:* A singular test asserting that `SUM(line_amount)` per purchase order always equals the PO header's `total_amount` — a business rule too specific for a generic test.

**Q81. What does `dbt source freshness` actually check, and why does it matter?**
It checks how recently a source table was loaded (based on a configured loaded-at column) against a warn/error threshold — so you find out "the upstream vendor feed hasn't landed in 26 hours" *before* your models silently run on stale data and produce technically-successful-but-wrong output downstream.

**Q82. What is a dbt exposure, and why bother defining them?**
An exposure documents a downstream consumer of your models (a dashboard, an ML feature store, an app) in your dbt project. It powers impact analysis — before changing or removing a model, you can see exactly which dashboards/consumers depend on it, instead of finding out only after something breaks.

**Q83. What are dbt snapshots for, concretely?**
Implementing SCD Type 2 against a source that only exposes current state — dbt compares the current snapshot to the last one (via `check_cols` or `timestamp` strategy) and automatically inserts new versioned rows with `dbt_valid_from`/`dbt_valid_to` when it detects a change, without you writing that merge logic by hand.

**Q84. What's the difference between `ref()` and `source()`, and why does that distinction matter for the DAG?**
`source()` points at raw, not-dbt-managed tables (the actual landing/raw tables); `ref()` points at another dbt model. dbt uses every `ref()`/`source()` call across the project to automatically build the dependency graph (DAG), figure out build order, and support things like `dbt run --select state:modified+` for efficient, dependency-aware CI runs.

**Q85. What are seeds, and when are they appropriate (and when are they not)?**
Seeds are small, static, rarely-changing CSV files loaded via `dbt seed` (e.g., a country-code lookup, a manually maintained mapping table). Not appropriate for large or frequently-changing data — that should come through the normal ingestion pipeline, not be checked into the dbt repo as a CSV.

**Q86. How would you handle a schema change from an upstream source in dbt (e.g., a new column, or a column type change)?**
Use `on_schema_change` config on incremental models (`ignore`, `fail`, `append_new_columns`, `sync_all_columns`) to decide the behavior explicitly, rather than letting an incremental model silently break or silently drop the new column — plus source freshness/schema tests to catch it early.

**Q87. What does `dbt docs generate` give you, and why is that valuable beyond "nice to have"?**
An auto-generated documentation site with column-level descriptions and a visual lineage graph derived directly from the project's actual `ref()`/`source()` graph — meaning documentation can't drift out of sync with reality the way a manually-maintained wiki diagram can.

**Q88. What is `dbt build`, and how is it different from `dbt run` + `dbt test`?**
`dbt build` runs models, tests, snapshots, and seeds together *in DAG order*, so if a test on an upstream model fails, downstream models that depend on it are skipped rather than built on top of already-known-bad data — versus running `dbt run` then `dbt test` separately, which builds everything first regardless.

**Q89. What's a macro, and why use one instead of copy-pasting SQL across models?**
A macro is reusable Jinja+SQL logic (like a function) — e.g., a standard "cents to dollars" conversion, or a consistent way to generate a surrogate key hash. Centralizing it means a bug fix or business-rule change happens in one place instead of needing to be hunted down and repeated across every model that copy-pasted the same logic.
*Example:* `{{ dbt_utils.generate_surrogate_key(['vendor_id', 'valid_from']) }}` used consistently everywhere a dimension surrogate key is built, instead of a slightly-different hand-written hash in every model.

**Q90. How do dbt environments (dev/prod) typically work without duplicating all your SQL?**
Via target-based schema/database configuration (`{{ target.schema }}`, profiles) — the same model code compiles against a developer's own sandbox schema in dev and the shared production schema in CI/prod, so you're testing the *same* logic, just pointed at different environments.

**Q91. What's "slim CI," and why does it matter at scale?**
Using `state:modified` comparisons (against a stored manifest from the last successful prod run) to build and test only the models that actually changed (plus their downstream dependents), instead of rebuilding an entire large DAG on every pull request — massively cuts CI time and cost as a project grows.

**Q92. Why pair dbt with Snowflake specifically (ELT, not ETL)?**
dbt only ever issues SQL — it compiles models and pushes the actual computation down into the warehouse. Snowflake's elastic, pay-for-what-you-use compute is a natural fit for that "let the warehouse do the heavy lifting" philosophy, versus a traditional ETL tool that transforms data on a separate processing tier before loading.

---

## 10. SQL Scenarios — CTEs, Subqueries, Joins, Window Functions

**Q93. What is a CTE, and is it materialized (does Snowflake compute it once and reuse the result)?**
A CTE (`WITH x AS (...)`) is a named, readable way to structure a query, referenceable multiple times within the same statement. In Snowflake (as in most engines), a CTE is **not automatically materialized** — if referenced multiple times, it may be recomputed each time unless the optimizer chooses to reuse it. If a CTE is expensive and reused heavily, consider a temp table instead.

**Q94. CTE vs. derived table (subquery in the FROM clause) — is there a real difference?**
Functionally, they're largely equivalent in modern optimizers (both are inlined query blocks) — the practical difference is readability and reuse: a CTE can be named once and referenced multiple times later in the same query; a derived table is defined inline exactly where it's used and can't be reused by name elsewhere in that query.

**Q95. What's a recursive CTE, and give a data-engineering-relevant use case.**
A CTE that references itself to walk a hierarchy iteratively (anchor query + recursive query, `UNION ALL`, until no more rows are produced).
*Example:* Resolving a multi-level vendor/parent-company hierarchy (a subsidiary rolling up to a parent, which rolls up further) to attribute spend correctly at the ultimate-parent level.

**Q96. Correlated vs. non-correlated subquery — what's the difference, and what's the performance implication?**
A non-correlated subquery runs independently of the outer query (computed once). A correlated subquery references a column from the outer query, so conceptually it's re-evaluated per outer row — which can be much slower on large tables; it can often be rewritten as a JOIN or a window function for better performance.

**Q97. `IN` vs. `EXISTS` — when would you prefer one over the other?**
`EXISTS` typically short-circuits (stops at the first match) and handles NULLs more predictably; `NOT IN` is a classic gotcha — if the subquery's column contains even one NULL, `NOT IN` can unexpectedly return zero rows entirely. `NOT EXISTS` doesn't have that trap, so it's generally the safer choice for "not in" style filters.

**Q98. What is a SEMI JOIN and an ANTI JOIN? (Snowflake doesn't have that exact keyword — how do you express them?)**
A semi join returns rows from the left table that *have* a match on the right, without duplicating rows for multiple matches — expressed via `WHERE EXISTS (...)` or `IN`. An anti join returns left rows with *no* match on the right — expressed via `WHERE NOT EXISTS (...)` or a `LEFT JOIN ... WHERE right.key IS NULL`.
*Example:* Anti join to find purchase orders that have no corresponding invoice yet (open/unbilled POs).

**Q99. Explain the difference between INNER, LEFT, RIGHT, and FULL OUTER joins in one line each.**
INNER: only matching rows on both sides. LEFT: all left rows, matched right columns or NULLs if no match. RIGHT: mirror of LEFT. FULL OUTER: all rows from both sides, NULLs wherever there's no match on the other side.

**Q100. What's the difference between `GROUP BY` and a window function (`OVER (PARTITION BY ...)`)?**
`GROUP BY` collapses rows into one row per group. A window function computes an aggregate *per partition* but keeps every original row — so you can show both the row-level detail and a group-level aggregate side by side in the same result set.
*Example:* Showing each individual purchase-order line alongside that vendor's running total spend, in one query — not possible with plain `GROUP BY` alone.

**Q101. What does the `QUALIFY` clause do in Snowflake, and what problem does it solve?**
It lets you filter on a window function's result directly, without wrapping the query in an outer `SELECT * FROM (...) WHERE rn = 1` subquery.
```sql
SELECT *
FROM stg_purchase_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY po_number ORDER BY updated_at DESC) = 1;
```
This is a very common "get the latest row per key" deduplication pattern.

**Q102. `RANK()` vs. `DENSE_RANK()` vs. `ROW_NUMBER()` — what's the difference?**
`ROW_NUMBER()` always gives unique sequential numbers, even for ties. `RANK()` gives ties the same rank but then skips subsequent rank numbers (1,1,3). `DENSE_RANK()` gives ties the same rank without skipping (1,1,2).

**Q103. What are `LAG()`/`LEAD()` useful for in a data engineering context?**
Comparing a row to the previous/next row within a partition — classic uses: detecting a change in value between snapshots (feeding SCD Type 2 change-detection logic), computing period-over-period differences, or finding gaps in a sequence.
*Example:* Using `LAG(status) OVER (PARTITION BY po_number ORDER BY event_ts)` to detect exactly when a purchase order's status changed, to drive an SCD2 snapshot comparison.

**Q104. Write a query concept for "find vendors with no purchase orders in the last 90 days."**
An anti-join/NOT EXISTS pattern: select from `dim_vendor` where `NOT EXISTS (SELECT 1 FROM fact_purchase_order po WHERE po.vendor_id = dim_vendor.vendor_id AND po.order_date > DATEADD(day, -90, CURRENT_DATE))`.

---

## 11. AWS S3 Fundamentals for Data Engineering

**Q105. Why is S3 so commonly used as a landing/staging zone ahead of Snowflake?**
It's cheap, extremely durable, decoupled from compute, and integrates natively with Snowflake (external stages, Snowpipe auto-ingest via event notifications) — a natural place to land raw files from any source system before they're loaded and transformed.

**Q106. How does S3 → Snowpipe auto-ingest actually work end to end?**
A new object lands in an S3 prefix → an S3 event notification fires → that event goes to an SQS queue → Snowpipe (subscribed to that queue) picks it up and loads the new file automatically, typically within a minute, without any polling or manual COPY trigger.

**Q107. What's a Storage Integration in Snowflake, and why is it preferred over embedding AWS access keys directly?**
A Storage Integration lets Snowflake assume an IAM role to access S3, instead of storing long-lived AWS access-key/secret credentials inside Snowflake stage definitions — safer (no static secrets to leak or rotate manually) and centrally governable via IAM policy.

**Q108. How would you design an S3 prefix/partitioning layout for a large, frequently-loaded raw zone?**
Partition by a natural, coarse-to-fine time hierarchy (e.g., `s3://bucket/source_system/year=2026/month=08/day=30/`), which supports efficient lifecycle policies (e.g., transition older raw data to cheaper storage classes) and makes it easy to reprocess or audit a specific date range without scanning the entire bucket.

---

## 12. Query Debugging & Performance Tuning

**Q109. Walk through how you'd debug a long-running query in Snowflake, and what performance-tuning steps you'd take.**

Treat it as a methodical process, not a grab-bag of tricks — the structure of the answer matters as much as the tricks themselves:

1. **Confirm what's actually slow.** In Snowsight's Query History, check the split between **queued** time and **execution** time. If a query spent 4 minutes queued and 10 seconds executing, the problem is warehouse concurrency/sizing, not the SQL — and no amount of query rewriting fixes that.
2. **Open the Query Profile.** This is the single most important tool — it's a visual DAG of every operator (scan, join, aggregate, sort) with the % of total time each consumed. Go straight to the most expensive node rather than eyeballing the whole SQL text.
3. **Check for spilling.** The profile shows "Bytes spilled to local storage" and "to remote storage." Spilling means the warehouse ran out of memory for a sort/join/aggregation and had to page to disk (remote spill is much worse than local). Fix: a bigger warehouse (more memory per node), or reduce the working set (filter/aggregate earlier, select fewer columns before the expensive join).
4. **Check partition pruning.** Compare "partitions scanned" vs. "partitions total" on the scan operator. Scanning far more than expected usually means either a missing/ineffective clustering key on a huge table, or a filter that isn't "sargable" — e.g., `WHERE TO_DATE(created_at) = '2026-08-30'` prevents pruning on `created_at`, while `WHERE created_at >= '2026-08-30' AND created_at < '2026-08-31'` allows it.
5. **Check for join explosion or skew.** Watch "rows produced" jump abnormally at a join node (a missed join key or an unintended many-to-many relationship), or one partition/key dominating the work (e.g., one vendor with 40% of all rows), which causes uneven work distribution across the warehouse's compute nodes.
6. **Check for repeated recomputation.** A CTE referenced multiple times isn't automatically materialized — if it's expensive and reused, materializing it into a temp table (or a Dynamic Table if it's a recurring production pattern) can turn N recomputations into one.
7. **Change one thing at a time and compare like-for-like.** Don't resize the warehouse *and* rewrite the query *and* add a clustering key in the same test — you won't know which change actually helped. Compare bytes scanned / partitions scanned / credits used across runs, not just wall-clock time (a second run can look faster purely from warm caching, which can mislead you into crediting the wrong fix).
8. **Close the loop.** Once you've found the fix, document the root cause (e.g., "missing pruning on `order_date` due to a wrapped filter") so the same query — or the next person's similar one — doesn't get re-debugged from scratch.

*Analogy:* It's like diagnosing why a delivery is late — first check if the truck is stuck in the depot waiting for a dock (queueing) before you start investigating the delivery route (execution). Interviewers are usually listening for exactly that discipline — confirm where the time is actually going before reaching for a fix.

---

*End of guide. If you get a question that isn't here verbatim, don't panic — reason out loud from these underlying concepts (grain, freshness vs. cost, idempotency, why-this-tool-over-that-one). That's what's actually being evaluated.*
