#!/usr/bin/env python3
"""
Auto-generate SCD Type 2 Dynamic Tables for all tables in Snowflake RAW schema.
Run this AFTER the first data load has populated RAW tables.

For each table in RAW, creates:
  DIM.<TABLE>_SCD2    — full version history, one row per distinct version of a record
  DIM.<TABLE>_CURRENT — latest version per primary key, excludes soft-deleted rows

The row_hash uses MD5 of all non-metadata columns, so the DDL works for any
table schema without hardcoding column names.

For tables with a known primary key: set REDSHIFT_PK_COLUMN (default: first column).
For tables without a clear PK: SCD2 will deduplicate on the full row hash,
which is safe but may create spurious versions if IDs are reused.

Tables are created in batches to avoid overwhelming the warehouse with
simultaneous ON_CREATE refreshes at 1500-table scale.

Usage:
  # After first data load:
  python setup/06_create_scd2_tables.py

  # To process only specific tables:
  INCLUDE_TABLES=customers,orders python setup/06_create_scd2_tables.py

  # To skip specific tables:
  EXCLUDE_TABLES=audit_log,temp_staging python setup/06_create_scd2_tables.py
"""
import os
import sys
import time
from pathlib import Path

import snowflake.connector

# Load .env
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# Config
SF_DATABASE       = os.getenv("SF_DATABASE", "REDSHIFT_DEMO")
SF_SCHEMA_RAW     = os.getenv("SF_SCHEMA_RAW", "RAW")
SF_SCHEMA_DIM     = os.getenv("SF_SCHEMA_DIM", "DIM")
SF_WAREHOUSE      = os.getenv("SF_WAREHOUSE", "")
SF_RUNTIME_ROLE   = os.getenv("SF_RUNTIME_ROLE", "OPENFLOW_RUNTIME_ROLE_MYSQL")
SCD2_TARGET_LAG   = os.getenv("SCD2_TARGET_LAG", "5 minutes")
BATCH_SIZE        = int(os.getenv("SCD2_CREATE_BATCH_SIZE", "50"))
WATERMARK_COLUMN  = os.getenv("WATERMARK_COLUMN", "updated_at")

INCLUDE_TABLES = [t.strip().upper() for t in os.getenv("INCLUDE_TABLES", "").split(",") if t.strip()]
EXCLUDE_TABLES = [t.strip().upper() for t in os.getenv("EXCLUDE_TABLES", "").split(",") if t.strip()]

# These metadata columns are added by OpenFlow and should NOT be part of the row hash
METADATA_COLS = {"SOURCE_SYSTEM", "SOURCE_TABLE", "SOURCE_SCHEMA", "INGESTED_AT", "REPLICATION_MODE"}

if not SF_WAREHOUSE:
    print("ERROR: SF_WAREHOUSE is required. Set it in .env")
    sys.exit(1)

def scd2_ddl(db, raw_schema, dim_schema, table, data_cols, wm_col, lag, wh):
    """
    Generate SCD2 Dynamic Table DDL for a given table.

    Uses MD5 of all non-metadata source columns as row_hash.
    Timestamps are handled as either epoch-millis (INTEGER) or native TIMESTAMP.
    The LEAD/LAG version chain closes the EFFECTIVE_TO window automatically.
    """
    upper_cols  = [f'"{c}"' for c in data_cols]
    hash_parts  = " || '|' || ".join([f'COALESCE(CAST("{c}" AS VARCHAR), \'\')' for c in data_cols])
    has_deleted = "IS_DELETED" in [c.upper() for c in data_cols]

    # Watermark expression — handle both epoch-millis (integer) and native timestamp
    wm_expr = f"""
        CASE
            WHEN TRY_CAST("{wm_col}" AS NUMBER) IS NOT NULL
            THEN TO_TIMESTAMP(CAST("{wm_col}" AS NUMBER) / 1000)
            ELSE TRY_TO_TIMESTAMP("{wm_col}")
        END""" if wm_col else f'ingested_at'

    is_deleted_expr = f'COALESCE(CAST("IS_DELETED" AS VARCHAR), \'false\')' if has_deleted else "'false'"

    col_list = ", ".join([f'"{c}"' for c in data_cols])

    return f"""
CREATE OR REPLACE DYNAMIC TABLE {db}.{dim_schema}.{table}_SCD2
    TARGET_LAG = '{lag}'
    WAREHOUSE = {wh}
    INITIALIZE = ON_CREATE
AS
WITH source_data AS (
    SELECT
        {col_list},
        ingested_at,
        {wm_expr} AS version_ts,
        {is_deleted_expr} AS is_deleted_flag,
        MD5({hash_parts}) AS row_hash
    FROM {db}.{raw_schema}.{table}
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY row_hash
            ORDER BY version_ts
        ) AS dup_rank
    FROM source_data
    WHERE version_ts IS NOT NULL
),
unique_versions AS (
    SELECT {col_list}, version_ts, is_deleted_flag, row_hash
    FROM deduped
    WHERE dup_rank = 1
),
versioned AS (
    SELECT
        {col_list},
        is_deleted_flag::BOOLEAN          AS is_deleted,
        version_ts                         AS effective_from,
        LEAD(version_ts) OVER (
            ORDER BY version_ts
        )                                  AS next_version_ts,
        ROW_NUMBER() OVER (ORDER BY version_ts DESC) AS rev_rank,
        ROW_NUMBER() OVER (ORDER BY version_ts ASC)  AS version
    FROM unique_versions
)
SELECT
    {col_list},
    IS_DELETED,
    EFFECTIVE_FROM,
    COALESCE(NEXT_VERSION_TS, '9999-12-31'::TIMESTAMP) AS effective_to,
    CASE WHEN REV_RANK = 1 THEN TRUE ELSE FALSE END    AS is_current,
    VERSION
FROM versioned;
""".strip()


def current_ddl(db, raw_schema, dim_schema, table, lag, wh):
    return f"""
CREATE OR REPLACE DYNAMIC TABLE {db}.{dim_schema}.{table}_CURRENT
    TARGET_LAG = '{lag}'
    WAREHOUSE = {wh}
    INITIALIZE = ON_CREATE
AS
SELECT * EXCLUDE (IS_DELETED, EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT, VERSION)
FROM {db}.{dim_schema}.{table}_SCD2
WHERE IS_CURRENT = TRUE
  AND IS_DELETED = FALSE;
""".strip()


print(f"=== SCD2 Dynamic Table Generator ===")
print(f"  Database:    {SF_DATABASE}")
print(f"  Source:      {SF_SCHEMA_RAW}")
print(f"  Target:      {SF_SCHEMA_DIM}")
print(f"  Warehouse:   {SF_WAREHOUSE}")
print(f"  Target lag:  {SCD2_TARGET_LAG}")
print(f"  Batch size:  {BATCH_SIZE}")
print(f"  Watermark:   {WATERMARK_COLUMN or '(ingested_at fallback)'}")

print(f"\n=== Step 1: Connect to Snowflake ===")
conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or ""
)
cur = conn.cursor()
cur.execute("USE ROLE ACCOUNTADMIN")
cur.execute(f"USE DATABASE {SF_DATABASE}")
print(f"  Connected")

print(f"\n=== Step 2: Discover tables in {SF_SCHEMA_RAW} ===")
cur.execute(f"SHOW TABLES IN SCHEMA {SF_DATABASE}.{SF_SCHEMA_RAW}")
all_tables = [row[1].upper() for row in cur.fetchall()]

if INCLUDE_TABLES:
    tables = [t for t in all_tables if t in INCLUDE_TABLES]
    print(f"  Filtered to INCLUDE_TABLES: {len(tables)} tables")
elif EXCLUDE_TABLES:
    tables = [t for t in all_tables if t not in EXCLUDE_TABLES]
    print(f"  Excluded {len(EXCLUDE_TABLES)} tables: {len(tables)} remaining")
else:
    tables = all_tables
    print(f"  Found {len(tables)} tables")

print(f"\n=== Step 3: Get column metadata for {len(tables)} tables ===")
table_cols = {}
for tbl in tables:
    cur.execute(f"DESCRIBE TABLE {SF_DATABASE}.{SF_SCHEMA_RAW}.{tbl}")
    cols = [row[0].upper() for row in cur.fetchall()
            if row[0].upper() not in METADATA_COLS]
    table_cols[tbl] = cols

print(f"  Column metadata loaded")

print(f"\n=== Step 4: Ensure DIM schema exists ===")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA_DIM}")
print(f"  {SF_DATABASE}.{SF_SCHEMA_DIM} ready")

print(f"\n=== Step 5: Create Dynamic Tables in batches of {BATCH_SIZE} ===")
created_scd2 = 0
created_curr = 0
failed = 0

for batch_start in range(0, len(tables), BATCH_SIZE):
    batch = tables[batch_start:batch_start + BATCH_SIZE]
    batch_num = batch_start // BATCH_SIZE + 1
    total_batches = (len(tables) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n  Batch {batch_num}/{total_batches} ({len(batch)} tables)...")

    for tbl in batch:
        cols = table_cols.get(tbl, [])
        if not cols:
            print(f"    SKIP {tbl}: no data columns found")
            failed += 1
            continue
        wm = WATERMARK_COLUMN if WATERMARK_COLUMN.upper() in [c.upper() for c in cols] else ""
        try:
            ddl = scd2_ddl(SF_DATABASE, SF_SCHEMA_RAW, SF_SCHEMA_DIM, tbl, cols, wm, SCD2_TARGET_LAG, SF_WAREHOUSE)
            cur.execute(ddl)
            created_scd2 += 1
        except Exception as e:
            print(f"    FAIL {tbl}_SCD2: {str(e)[:120]}")
            failed += 1
            continue
        try:
            ddl = current_ddl(SF_DATABASE, SF_SCHEMA_RAW, SF_SCHEMA_DIM, tbl, SCD2_TARGET_LAG, SF_WAREHOUSE)
            cur.execute(ddl)
            created_curr += 1
        except Exception as e:
            print(f"    FAIL {tbl}_CURRENT: {str(e)[:120]}")

    print(f"    Batch done — {created_scd2} SCD2, {created_curr} CURRENT so far")
    if batch_start + BATCH_SIZE < len(tables):
        print(f"    Waiting 5s before next batch (avoids warehouse saturation)...")
        time.sleep(5)

print(f"\n=== Step 6: Grant access to {SF_RUNTIME_ROLE} ===")
try:
    cur.execute(f"GRANT USAGE ON SCHEMA {SF_DATABASE}.{SF_SCHEMA_DIM} TO ROLE {SF_RUNTIME_ROLE}")
    cur.execute(f"GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA {SF_DATABASE}.{SF_SCHEMA_DIM} TO ROLE {SF_RUNTIME_ROLE}")
    cur.execute(f"GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA {SF_DATABASE}.{SF_SCHEMA_DIM} TO ROLE {SF_RUNTIME_ROLE}")
    print(f"  Granted SELECT on {SF_DATABASE}.{SF_SCHEMA_DIM} to {SF_RUNTIME_ROLE}")
except Exception as e:
    print(f"  WARN: grant failed: {e}")

cur.close()
conn.close()

print(f"""
=== DONE ===
  SCD2 tables created:    {created_scd2}
  CURRENT views created:  {created_curr}
  Failed:                 {failed}
  Total DIM objects:      {created_scd2 + created_curr}

  Query history:
    SELECT "customer_id", "company_name", EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT, VERSION
    FROM {SF_DATABASE}.{SF_SCHEMA_DIM}.CUSTOMERS_SCD2
    ORDER BY "customer_id", VERSION;

  Current state:
    SELECT * FROM {SF_DATABASE}.{SF_SCHEMA_DIM}.CUSTOMERS_CURRENT;
""")
