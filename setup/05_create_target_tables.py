#!/usr/bin/env python3
"""
Auto-generate Snowflake RAW target tables from Redshift source metadata.
Queries information_schema in Redshift to discover all tables and columns,
then creates matching tables in Snowflake's RAW schema.

Handles 1500+ tables efficiently:
  - Queries Redshift metadata once via AWS Data API
  - Batches Snowflake DDL execution
  - Adds metadata columns (source_system, source_table, source_schema, ingested_at)
  - Grants access to the OpenFlow runtime role
  - Optionally enables CHANGE_TRACKING (required for scd2 mode Dynamic Tables)

Usage:
  python setup/05_create_target_tables.py

Set REPLICATION_MODE=scd2 in .env to also enable CHANGE_TRACKING on all tables.
"""
import json
import os
import subprocess
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
MODE             = os.getenv("REPLICATION_MODE", "scd2")
AWS_PROFILE      = os.getenv("AWS_PROFILE", "default")
AWS_REGION       = os.getenv("AWS_REGION", "us-west-2")
REDSHIFT_WG      = os.getenv("REDSHIFT_WORKGROUP", "openflow-demo-wg")
REDSHIFT_DB      = os.getenv("REDSHIFT_DB", "demo")
REDSHIFT_SCHEMA  = os.getenv("REDSHIFT_SCHEMA", "sales")
SF_CONN          = os.getenv("SNOWFLAKE_CONNECTION_NAME", "")
SF_DATABASE      = os.getenv("SF_DATABASE", "REDSHIFT_DEMO")
SF_SCHEMA_RAW    = os.getenv("SF_SCHEMA_RAW", "RAW")
SF_RUNTIME_ROLE  = os.getenv("SF_RUNTIME_ROLE", "OPENFLOW_RUNTIME_ROLE_MYSQL")
ENABLE_CT        = (MODE == "scd2")  # CHANGE_TRACKING required for Dynamic Tables

# Redshift→Snowflake type mapping
RS_TO_SF = {
    "integer":                    "NUMBER",
    "bigint":                     "NUMBER",
    "smallint":                   "NUMBER",
    "double precision":           "FLOAT",
    "real":                       "FLOAT",
    "boolean":                    "BOOLEAN",
    "character varying":          "VARCHAR",
    "character":                  "VARCHAR",
    "text":                       "VARCHAR",
    "date":                       "DATE",
    "timestamp without time zone":"TIMESTAMP_NTZ",
    "timestamp with time zone":   "TIMESTAMP_TZ",
    "time without time zone":     "TIME",
}

METADATA_COLS = [
    "source_system    VARCHAR",
    "source_table     VARCHAR",
    "source_schema    VARCHAR",
    "ingested_at      TIMESTAMP_NTZ",
]

ENV = {**os.environ, "AWS_PROFILE": AWS_PROFILE}

def rs_query(sql):
    r = subprocess.run(
        ["aws", "redshift-data", "execute-statement",
         "--workgroup-name", REDSHIFT_WG, "--database", REDSHIFT_DB,
         "--sql", sql, "--region", AWS_REGION,
         "--query", "Id", "--output", "text"],
        capture_output=True, text=True, env=ENV)
    stmt_id = r.stdout.strip()
    if not stmt_id:
        print(f"  ERROR: {r.stderr[:200]}")
        sys.exit(1)
    for _ in range(20):
        time.sleep(3)
        r2 = subprocess.run(
            ["aws", "redshift-data", "describe-statement",
             "--id", stmt_id, "--region", AWS_REGION, "--output", "json"],
            capture_output=True, text=True, env=ENV)
        d = json.loads(r2.stdout)
        if d.get("Status") == "FINISHED":
            break
        if d.get("Status") == "FAILED":
            print(f"  SQL ERROR: {d.get('Error', 'unknown')}")
            sys.exit(1)
    r3 = subprocess.run(
        ["aws", "redshift-data", "get-statement-result",
         "--id", stmt_id, "--region", AWS_REGION, "--output", "json"],
        capture_output=True, text=True, env=ENV)
    d = json.loads(r3.stdout)
    return d.get("Records", [])

def map_type(rs_type, char_max_length, numeric_precision, numeric_scale):
    rs_type = rs_type.lower()
    base = RS_TO_SF.get(rs_type, "VARCHAR")
    if base == "VARCHAR" and char_max_length:
        return f"VARCHAR({char_max_length})"
    if base == "NUMBER" and numeric_precision:
        if numeric_scale and int(numeric_scale) > 0:
            return f"NUMBER({numeric_precision},{numeric_scale})"
        return f"NUMBER({numeric_precision})"
    return base

print(f"=== Step 1: Discover tables in Redshift {REDSHIFT_SCHEMA}.* ===")
tables_raw = rs_query(
    f"SELECT DISTINCT table_name FROM information_schema.tables "
    f"WHERE table_schema = '{REDSHIFT_SCHEMA}' AND table_type = 'BASE TABLE' "
    f"ORDER BY table_name"
)
tables = [r[0]["stringValue"] for r in tables_raw]
print(f"  Found {len(tables)} tables: {tables[:10]}{'...' if len(tables) > 10 else ''}")

print(f"\n=== Step 2: Get column metadata for all tables ===")
cols_raw = rs_query(
    f"SELECT table_name, column_name, data_type, character_maximum_length, "
    f"numeric_precision, numeric_scale, ordinal_position "
    f"FROM information_schema.columns "
    f"WHERE table_schema = '{REDSHIFT_SCHEMA}' "
    f"ORDER BY table_name, ordinal_position"
)

schema = {}
for r in cols_raw:
    tbl  = r[0]["stringValue"]
    col  = r[1]["stringValue"]
    typ  = r[2]["stringValue"]
    cml  = r[3].get("longValue") or r[3].get("stringValue")
    prec = r[4].get("longValue") or r[4].get("stringValue")
    scl  = r[5].get("longValue") or r[5].get("stringValue")
    schema.setdefault(tbl, []).append((col, map_type(typ, cml, prec, scl)))

print(f"  Column metadata loaded for {len(schema)} tables")

print(f"\n=== Step 3: Connect to Snowflake ===")
conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or SF_CONN
)
cur = conn.cursor()
cur.execute("USE ROLE ACCOUNTADMIN")
cur.execute(f"USE DATABASE {SF_DATABASE}")
cur.execute(f"USE SCHEMA {SF_SCHEMA_RAW}")
print(f"  Connected → {SF_DATABASE}.{SF_SCHEMA_RAW}")

print(f"\n=== Step 4: Create {len(tables)} target tables in Snowflake RAW ===")
created = 0
skipped = 0
failed  = 0

for tbl in tables:
    cols = schema.get(tbl, [])
    if not cols:
        print(f"  WARN: No columns found for {tbl}, skipping")
        skipped += 1
        continue

    col_defs = ",\n    ".join(f'"{col}" {sf_type}' for col, sf_type in cols)
    meta_defs = ",\n    ".join(METADATA_COLS)

    ddl = f"""CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA_RAW}.{tbl.upper()} (
    {col_defs},
    {meta_defs}
)"""
    try:
        cur.execute(ddl)
        created += 1
        if created % 50 == 0:
            print(f"  ... {created}/{len(tables)} tables created")
    except Exception as e:
        print(f"  FAIL {tbl}: {e}")
        failed += 1

print(f"  Done: {created} created, {skipped} skipped, {failed} failed")

print(f"\n=== Step 5: Grant access to {SF_RUNTIME_ROLE} ===")
cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA {SF_DATABASE}.{SF_SCHEMA_RAW} TO ROLE {SF_RUNTIME_ROLE}")
cur.execute(f"GRANT ALL ON FUTURE TABLES IN SCHEMA {SF_DATABASE}.{SF_SCHEMA_RAW} TO ROLE {SF_RUNTIME_ROLE}")
print(f"  Granted ALL on {SF_DATABASE}.{SF_SCHEMA_RAW} to {SF_RUNTIME_ROLE}")

if ENABLE_CT:
    print(f"\n=== Step 6: Enable CHANGE_TRACKING on all {len(tables)} tables (scd2 mode) ===")
    ct_ok = 0
    ct_fail = 0
    for tbl in tables:
        try:
            cur.execute(f"ALTER TABLE {SF_DATABASE}.{SF_SCHEMA_RAW}.{tbl.upper()} SET CHANGE_TRACKING = ON")
            ct_ok += 1
        except Exception as e:
            print(f"  FAIL CT {tbl}: {e}")
            ct_fail += 1
    print(f"  CHANGE_TRACKING enabled: {ct_ok} ok, {ct_fail} failed")
else:
    print(f"\n  Skipping CHANGE_TRACKING (not needed for mode={MODE})")

cur.close()
conn.close()

print(f"""
=== DONE ===
  Created {created} tables in {SF_DATABASE}.{SF_SCHEMA_RAW}
  Mode: {MODE}
  CHANGE_TRACKING: {'enabled' if ENABLE_CT else 'skipped'}

  Next:
    1. Run connector/build_flow.py to build the NiFi flow
    2. Let the first data load complete
""" + ("    3. Run setup/06_create_scd2_tables.py to add SCD2 Dynamic Tables\n" if MODE == "scd2" else ""))
