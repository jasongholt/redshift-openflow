# Cortex Code Starter Prompt — Redshift-to-Snowflake OpenFlow Multi-Mode Replication

Paste this prompt into a Cortex Code session to begin or reproduce this project.
Choose the option that matches your goal and environment.

| Option | AWS | Replication mode |
|--------|-----|-----------------|
| **A — Full Setup** | Needs to be created | CDC + SCD2 or Gold Mirror |
| **B — Connector Only** | Already running | CDC + SCD2 or Gold Mirror |
| **C — Gold Mirror** | Already running | Full refresh, no CDC/SCD2 |

**Set `REPLICATION_MODE` in your `.env`** to control which mode the flow uses:
- `scd2` — incremental CDC + SCD2 Dynamic Tables (full version history)
- `cdc` — incremental CDC only (RAW tables, no history)
- `gold_mirror` — full truncate+reload on schedule. Optional watermark: if `WATERMARK_COLUMN` is set, only reloads tables that have changed since the last run (change-detection gate). If blank, every table reloads every cycle.

---

## Option A — Full Setup (AWS + Snowflake + Connector)

```
I want to build a custom Snowflake OpenFlow JDBC connector that replicates tables
from Amazon Redshift Serverless into Snowflake with watermark-based incremental sync
and SCD Type 2 history tracking. This is a demo project for showing OpenFlow custom
connector capabilities.

Here is what the full architecture looks like:

  Amazon Redshift Serverless (private, no public access)
    → Network Load Balancer (internal, port 5439)
    → AWS VPC Endpoint Service
    → Snowflake PrivateLink endpoint
    → OpenFlow Runtime (NiFi on SPCS)
        ListDatabaseTables    discovers all tables in the source schema
        GenerateTableFetch    generates paginated watermark SQL queries per table
        ExecuteSQL            executes queries with 10 concurrent tasks
        ConvertRecord         converts Avro output to JSON
        UpdateRecord          adds source_system, source_table, ingested_at metadata
        PutSnowpipeStreaming   writes to Snowflake using dynamic table routing (Table=${db.table.name})
    → REDSHIFT_DEMO.RAW.*               raw landing tables (one per source table)
    → REDSHIFT_DEMO.DIM.CUSTOMERS_SCD2  SCD Type 2 Dynamic Table (full version history)
    → REDSHIFT_DEMO.DIM.CUSTOMERS_CURRENT  latest-state Dynamic Table (active records only)

The project repo is already cloned. All scripts and config live in:
  redshift-openflow-scd2/

Key files:
  .env.example              — all configuration variables with instructions
  setup/01_aws_redshift.sh  — creates Redshift Serverless namespace, workgroup, NLB, VPC Endpoint Service
  setup/02_snowflake_networking.sql — creates Network Rule, External Access Integration (EAI)
  setup/03_snowflake_objects.sql    — creates database, schemas, target tables, SCD2 Dynamic Tables
  setup/04_seed_redshift.sql        — seeds Redshift with sample data (customers, orders, products)
  connector/build_flow.py           — builds the complete NiFi flow via NiFi REST API
  tests/parity_test.py              — 15-test suite validating Redshift == Snowflake data parity
  tests/scd2_test_suite.py          — 13-test SCD2 mutation suite (multi-update, soft delete, bulk insert)

Please start by:
1. Copying .env.example to .env — set REPLICATION_MODE, fill all REQUIRED values
2. Running setup/01_aws_redshift.sh to create the AWS infrastructure
3. Creating the PrivateLink endpoint in Snowflake (manual step)
4. Running setup/02_snowflake_networking.sql and setup/03_snowflake_objects.sql as ACCOUNTADMIN
5. Creating the OpenFlow runtime via Control Plane UI (manual — cannot be scripted)
6. Running setup/04_seed_redshift.sql to load sample data
7. Extracting the JDBC driver: mkdir -p /tmp/redshift-jdbc && unzip drivers/redshift-jdbc42-2.2.5.zip -d /tmp/redshift-jdbc
8. Running python setup/05_create_target_tables.py to auto-create all Snowflake target tables
9. Running python connector/build_flow.py to build the NiFi flow
10. Validating with: python tests/parity_test.py
11. (scd2 only) After first data load: python setup/06_create_scd2_tables.py

Use the $openflow skill for any NiFi/nipyapi patterns, processor configuration, or EAI setup.

Important lessons from the original build:
- GTF Table Name MUST be schema-qualified: ${db.table.schema}.${db.table.name}
  (ListDatabaseTables emits the table name without schema prefix)
- nipyapi create_parameter_context() returns HTTP 500 on some runtimes — use raw NiFi REST API instead
- PutSnowpipeStreaming target tables must exist in Snowflake before data flows
- UpdateRecord literal-value strategy cannot reference other fields — use QueryRecord SQL for cross-field transforms
- Redshift passwords with special characters (!, @) cause JDBC auth failures — use alphanumeric only
- NiFi ProcessorsApi.clear_state is version-suffixed as clear_state3() in nipyapi
```

---

## Option C — Simple Replication (Redshift as Gold Layer)

Use this when Redshift is already the curated gold layer and you just want a
faithful mirror in Snowflake. No CDC complexity, no SCD2.
Every changed table is truncated and reloaded in full on a schedule.
Optionally, set WATERMARK_COLUMN to detect which tables have changed before reloading
(avoids full scans on stable tables), or leave it blank to reload everything every cycle.

```
I want to replicate tables from Amazon Redshift Serverless into Snowflake as-is
using Snowflake OpenFlow (hosted NiFi on SPCS). Redshift is our gold layer — tables
are already clean and curated. I do not need CDC or SCD2. I just want a reliable,
scheduled mirror of those tables in Snowflake so downstream tools can query them.

Requirements:
- All tables in the Redshift `sales` schema should be replicated to Snowflake
- Full refresh per table on a schedule (e.g., every 15 minutes)
- No watermarking or incremental logic — truncate and reload is acceptable for small tables,
  or use a simple full extract if tables are larger
- No transformation — land data exactly as it comes from Redshift
- Data should be queryable in Snowflake as REDSHIFT_DEMO.RAW.<table_name>

AWS infrastructure is already in place:
  - Redshift Serverless workgroup is running
  - NLB forwards port 5439 to Redshift ENIs
  - VPC Endpoint Service exists for PrivateLink

The project repo is cloned at redshift-openflow-scd2/.

Relevant files:
  .env.example                        — all config variables with instructions
  setup/02_snowflake_networking.sql   — Network Rule + EAI (run as ACCOUNTADMIN)
  setup/03_snowflake_objects.sql      — database, schemas, grants
  setup/05_create_target_tables.py    — auto-creates all Snowflake RAW target tables
  setup/06_create_scd2_tables.py      — generates SCD2 Dynamic Tables (scd2 mode only)
  connector/build_flow.py             — NiFi flow builder (mode-aware via REPLICATION_MODE)
  drivers/redshift-jdbc42-2.2.5.zip   — Redshift JDBC driver

Please start by:
1. Copying .env.example to .env — set REPLICATION_MODE, fill all REQUIRED values
2. Running setup/02_snowflake_networking.sql (update NLB_DNS at the top first)
3. Running setup/03_snowflake_objects.sql as ACCOUNTADMIN
4. Creating the OpenFlow runtime via Control Plane UI (manual — cannot be scripted):
   - Size: Medium for dev, Large for production
   - Max Nodes: 3
   - Attach the EAI created in step 2
5. Extracting the JDBC driver: unzip drivers/redshift-jdbc42-2.2.5.zip -d /tmp/redshift-jdbc
6. Running python setup/05_create_target_tables.py to auto-create all Snowflake target tables
7. Running python connector/build_flow.py to build the NiFi flow (mode from .env)
8. Validating: python tests/parity_test.py
9. (scd2 only) After first data load: python setup/06_create_scd2_tables.py

Important lessons that still apply:
- GTF Table Name MUST be schema-qualified: ${db.table.schema}.${db.table.name}
  (ListDatabaseTables emits table name without schema prefix — this will silently produce 0 rows if wrong)
- nipyapi create_parameter_context() returns HTTP 500 on some runtimes — use raw NiFi REST API instead
- PutSnowpipeStreaming target tables must exist in Snowflake before data flows
- Redshift passwords with special characters (!, @) cause JDBC auth failures — use alphanumeric only
```

---

## Option B — AWS Already Exists (Snowflake + Connector Only)

```
I want to build a custom Snowflake OpenFlow JDBC connector that replicates tables
from an existing Amazon Redshift Serverless instance into Snowflake with
watermark-based incremental sync and SCD Type 2 history tracking.

AWS infrastructure is already in place:
  - Redshift Serverless workgroup is running and accessible
  - A Network Load Balancer (NLB) forwards port 5439 to the Redshift ENIs
  - A VPC Endpoint Service exists for PrivateLink

I need to provide you with:
  - NLB_DNS: the DNS name of the NLB (port 5439)
  - REDSHIFT_ADMIN_USER and REDSHIFT_ADMIN_PASSWORD
  - My Snowflake account and warehouse names

The project repo is already cloned. All scripts and config live in:
  redshift-openflow-scd2/

Key files:
  .env.example              — all configuration variables with instructions
  setup/02_snowflake_networking.sql — creates Network Rule, External Access Integration (EAI)
  setup/03_snowflake_objects.sql    — creates database, schemas, target tables, SCD2 Dynamic Tables
  setup/04_seed_redshift.sql        — seeds Redshift with sample data (optional if data already exists)
  connector/build_flow.py           — builds the complete NiFi flow via NiFi REST API
  tests/parity_test.py              — 15-test suite validating Redshift == Snowflake data parity
  tests/scd2_test_suite.py          — 13-test SCD2 mutation suite

The NiFi flow architecture:
  ListDatabaseTables    discovers all tables in the source schema (dynamic, no per-table config)
  GenerateTableFetch    generates paginated watermark SQL queries (watermark column: updated_at)
  ExecuteSQL            executes queries with 10 concurrent tasks
  ConvertRecord         converts Avro to JSON
  UpdateRecord          adds source_system, source_table, ingested_at
  PutSnowpipeStreaming   writes to Snowflake with Table=${db.table.name} (dynamic routing)

Please start by:
1. Copying .env.example to .env and helping me fill in my values (skip AWS_ and NLB_ sections)
2. Running setup/02_snowflake_networking.sql (update NLB_DNS at the top of the file first)
3. Creating the OpenFlow runtime via the Control Plane UI (manual — cannot be scripted):
   - Size: Medium for dev, Large for production
   - Max nodes: 3 (auto-scales)
   - Attach the EAI created in step 2
4. Running setup/03_snowflake_objects.sql to create Snowflake objects
5. Seeding Redshift if needed: setup/04_seed_redshift.sql
6. Extracting the JDBC driver: mkdir -p /tmp/redshift-jdbc && cd /tmp/redshift-jdbc && unzip drivers/redshift-jdbc42-2.2.5.zip
7. Running connector/build_flow.py to build the NiFi flow
8. Validating with tests/parity_test.py

Use the $openflow skill for any NiFi/nipyapi patterns, processor configuration, or EAI setup.

Important lessons from the original build:
- GTF Table Name MUST be schema-qualified: ${db.table.schema}.${db.table.name}
  (ListDatabaseTables emits the table name without schema prefix)
- nipyapi create_parameter_context() returns HTTP 500 on some runtimes — use raw NiFi REST API instead
- PutSnowpipeStreaming target tables must exist in Snowflake before data flows
- UpdateRecord literal-value strategy cannot reference other fields — use QueryRecord SQL for cross-field transforms
- Redshift passwords with special characters (!, @) cause JDBC auth failures — use alphanumeric only
- NiFi ProcessorsApi.clear_state is version-suffixed as clear_state3() in nipyapi
```
