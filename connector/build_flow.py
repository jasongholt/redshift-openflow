#!/usr/bin/env python3
"""
Redshift-to-Snowflake OpenFlow Custom JDBC Connector
=====================================================

Builds a NiFi flow that replicates Redshift tables to Snowflake.
Supports three modes controlled by REPLICATION_MODE in .env:

  gold_mirror  Full extract on schedule. Truncates target before each load.
               No watermark column required.

  cdc          Incremental extract via updated_at watermark.
               Appends only new/changed rows. Requires updated_at on all tables.

  scd2         Same as cdc. SCD Type 2 Dynamic Tables are handled separately
               by setup/06_create_scd2_tables.py — the NiFi flow is identical to cdc.

Flow: ListDatabaseTables → GenerateTableFetch → ExecuteSQL → ConvertRecord
      → [ExecuteScript (gold_mirror truncate)] → UpdateRecord → PutSnowpipeStreaming

Prerequisites:
  - OpenFlow runtime created with EAI attached (manual via Control Plane UI)
  - JDBC driver extracted: unzip drivers/redshift-jdbc42-*.zip -d $JDBC_JAR_DIR
  - Target tables pre-created: python setup/05_create_target_tables.py
  - For scd2: python setup/06_create_scd2_tables.py after first data load

Usage:
  cp .env.example .env   # fill in your values
  python connector/build_flow.py
"""
import json
import glob
import os
import ssl
import time
import urllib.request
from pathlib import Path

# Load .env from repo root
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# =============================================================================
# CONFIGURATION — all values come from .env
# =============================================================================

MODE = os.getenv("REPLICATION_MODE", "scd2")  # gold_mirror | cdc | scd2
if MODE not in ("gold_mirror", "cdc", "scd2"):
    print(f"ERROR: REPLICATION_MODE must be gold_mirror, cdc, or scd2. Got: {MODE}")
    exit(1)

print(f"\n  Mode: {MODE.upper()}")

OPENFLOW_HOST = os.getenv("OPENFLOW_HOST", "")
OPENFLOW_RUNTIME = os.getenv("OPENFLOW_RUNTIME", "")
NIFI_BASE_URL = f"https://{OPENFLOW_HOST}/{OPENFLOW_RUNTIME}/nifi-api"
NIFI_PAT = os.getenv("NIFI_PAT", "")

JDBC_JAR_DIR = os.getenv("JDBC_JAR_DIR", "/tmp/redshift-jdbc")

NLB_DNS = os.getenv("NLB_DNS", "localhost")
NLB_PORT = os.getenv("NLB_PORT", "5439")
REDSHIFT_DB = os.getenv("REDSHIFT_DB", "demo")
REDSHIFT_URL = f"jdbc:redshift://{NLB_DNS}:{NLB_PORT}/{REDSHIFT_DB}"
REDSHIFT_USER = os.getenv("REDSHIFT_ADMIN_USER", "admin")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_ADMIN_PASSWORD", "")

SF_DATABASE = os.getenv("SF_DATABASE", "REDSHIFT_DEMO")
SF_SCHEMA = os.getenv("SF_SCHEMA_RAW", "RAW")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "")
SF_ROLE = os.getenv("SF_RUNTIME_ROLE", "OPENFLOW_RUNTIME_ROLE_MYSQL")

REDSHIFT_SCHEMA_PATTERN = os.getenv("REDSHIFT_SCHEMA_PATTERN", os.getenv("REDSHIFT_SCHEMA", "sales"))
REDSHIFT_TABLE_PATTERN = os.getenv("REDSHIFT_TABLE_PATTERN", "%")

# Mode-specific GTF settings
if MODE == "gold_mirror":
    WATERMARK_COLUMN = ""           # No watermark — full extract every run
    GTF_PARTITION_SIZE = os.getenv("GTF_PARTITION_SIZE", "0")  # 0 = single query per table
    TRUNCATE_BEFORE_LOAD = os.getenv("TRUNCATE_BEFORE_LOAD", "true").lower() == "true"
else:  # cdc or scd2
    WATERMARK_COLUMN = os.getenv("WATERMARK_COLUMN", "updated_at")
    GTF_PARTITION_SIZE = os.getenv("GTF_PARTITION_SIZE", "10000")
    TRUNCATE_BEFORE_LOAD = False

DBCP_MAX_CONNECTIONS = int(os.getenv("DBCP_MAX_CONNECTIONS", "25"))
ESQL_CONCURRENT_TASKS = int(os.getenv("ESQL_CONCURRENT_TASKS", "10"))
PSS_CONCURRENT_TASKS = int(os.getenv("PSS_CONCURRENT_TASKS", "5"))
NIFI_QUEUE_OBJECT_THRESHOLD = os.getenv("NIFI_QUEUE_OBJECT_THRESHOLD", "50000")
NIFI_QUEUE_SIZE_THRESHOLD = os.getenv("NIFI_QUEUE_SIZE_THRESHOLD", "2 GB")

# =============================================================================
# NiFi REST API helpers
# =============================================================================

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {"Authorization": f"Bearer {NIFI_PAT}", "Content-Type": "application/json"}

def api(method, path, data=None):
    url = f"{NIFI_BASE_URL}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=HEADERS, method=method)
    with urllib.request.urlopen(req, context=SSL_CTX) as resp:
        return json.loads(resp.read().decode())

def upload_asset(ctx_id, file_path):
    url = f"{NIFI_BASE_URL}/parameter-contexts/{ctx_id}/assets"
    filename = file_path.split("/")[-1]
    with open(file_path, "rb") as f:
        body = f.read()
    hdrs = {"Authorization": f"Bearer {NIFI_PAT}", "Content-Type": "application/octet-stream", "filename": filename}
    req = urllib.request.Request(url, data=body, headers=hdrs, method="POST")
    with urllib.request.urlopen(req, context=SSL_CTX) as resp:
        return json.loads(resp.read().decode())

def create_controller(pg_id, type_str, name):
    types = api("GET", "/flow/controller-service-types")
    matching = [t for t in types["controllerServiceTypes"] if type_str in t["type"]]
    if not matching:
        raise RuntimeError(f"No controller type found for {type_str}")
    cs_type = matching[0]
    body = {"revision": {"version": 0}, "component": {"type": cs_type["type"], "bundle": cs_type["bundle"], "name": name}}
    result = api("POST", f"/process-groups/{pg_id}/controller-services", body)
    print(f"  Created controller {name}: {result['id']}")
    return result

def create_processor(pg_id, type_str, name, x=0, y=0):
    types = api("GET", "/flow/processor-types")
    matching = [t for t in types["processorTypes"] if t["type"].endswith(type_str)]
    if not matching:
        matching = [t for t in types["processorTypes"] if type_str in t["type"]]
    if not matching:
        raise RuntimeError(f"No processor type found for {type_str}")
    p_type = matching[0]
    body = {"revision": {"version": 0}, "component": {"type": p_type["type"], "bundle": p_type["bundle"], "name": name, "position": {"x": x, "y": y}}}
    result = api("POST", f"/process-groups/{pg_id}/processors", body)
    print(f"  Created processor {name}: {result['id']}")
    return result

def configure_processor(proc_id, properties=None, scheduling=None, auto_terminate=None):
    current = api("GET", f"/processors/{proc_id}")
    config = {}
    if properties:
        config["properties"] = properties
    if scheduling:
        config.update(scheduling)
    if auto_terminate:
        config["autoTerminatedRelationships"] = auto_terminate
    api("PUT", f"/processors/{proc_id}", {
        "revision": current["revision"], "id": proc_id,
        "component": {"id": proc_id, "config": config}
    })

def create_connection(source_id, dest_id, rels, pg_id, backpressure=False):
    body = {"revision": {"version": 0}, "component": {
        "source": {"id": source_id, "groupId": pg_id, "type": "PROCESSOR"},
        "destination": {"id": dest_id, "groupId": pg_id, "type": "PROCESSOR"},
        "selectedRelationships": rels,
    }}
    if backpressure:
        body["component"]["backPressureObjectThreshold"] = int(NIFI_QUEUE_OBJECT_THRESHOLD)
        body["component"]["backPressureDataSizeThreshold"] = NIFI_QUEUE_SIZE_THRESHOLD
    result = api("POST", f"/process-groups/{pg_id}/connections", body)
    print(f"  Connected [{', '.join(rels)}]")
    return result

def enable_controller(cs_id):
    current = api("GET", f"/controller-services/{cs_id}")
    api("PUT", f"/controller-services/{cs_id}/run-status", {
        "revision": current["revision"], "state": "ENABLED"
    })

def section(title):
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


# =============================================================================
# VALIDATION
# =============================================================================

errors = []
if not NIFI_PAT:
    errors.append("NIFI_PAT is not set — get from nipyapi profile or OpenFlow session")
if not OPENFLOW_HOST:
    errors.append("OPENFLOW_HOST is not set")
if not OPENFLOW_RUNTIME:
    errors.append("OPENFLOW_RUNTIME is not set")
if not REDSHIFT_PASSWORD:
    errors.append("REDSHIFT_ADMIN_PASSWORD is not set")
if not SF_WAREHOUSE:
    errors.append("SF_WAREHOUSE is not set")
if MODE in ("cdc", "scd2") and not WATERMARK_COLUMN:
    errors.append(f"WATERMARK_COLUMN must be set for mode '{MODE}'")

if errors:
    print("\nERROR: Missing required configuration:")
    for e in errors:
        print(f"  - {e}")
    print("\nFix these in your .env file and retry.")
    exit(1)


# =============================================================================
# MAIN FLOW BUILD
# =============================================================================

section(f"Step 1: Create Parameter Context + Upload JDBC Driver  [mode={MODE}]")

ctx_body = {
    "revision": {"version": 0},
    "component": {
        "name": f"Redshift Parameters ({MODE})",
        "parameters": [
            {"parameter": {"name": "redshift_url", "value": REDSHIFT_URL, "sensitive": False}},
            {"parameter": {"name": "redshift_user", "value": REDSHIFT_USER, "sensitive": False}},
            {"parameter": {"name": "redshift_password", "value": REDSHIFT_PASSWORD, "sensitive": True}},
            {"parameter": {"name": "sf_database", "value": SF_DATABASE, "sensitive": False}},
            {"parameter": {"name": "sf_schema", "value": SF_SCHEMA, "sensitive": False}},
            {"parameter": {"name": "sf_warehouse", "value": SF_WAREHOUSE, "sensitive": False}},
            {"parameter": {"name": "sf_role", "value": SF_ROLE, "sensitive": False}},
        ],
    },
}
result = api("POST", "/parameter-contexts", ctx_body)
CTX_ID = result["id"]
print(f"  Parameter Context: {CTX_ID}")

jar_files = sorted(glob.glob(os.path.join(JDBC_JAR_DIR, "*.jar")))
print(f"  Uploading {len(jar_files)} JDBC JARs...")
for jar in jar_files:
    try:
        upload_asset(CTX_ID, jar)
    except Exception as e:
        print(f"    Skip {os.path.basename(jar)}: {str(e)[:60]}")

existing = api("GET", f"/parameter-contexts/{CTX_ID}/assets")
all_assets = [{"id": a["asset"]["id"], "name": a["asset"]["name"]} for a in existing.get("assets", [])]
print(f"  {len(all_assets)} JAR assets uploaded")

current = api("GET", f"/parameter-contexts/{CTX_ID}")
params = current["component"]["parameters"]
params.append({"parameter": {"name": "redshift_driver", "sensitive": False, "value": None, "referencedAssets": all_assets}})
api("PUT", f"/parameter-contexts/{CTX_ID}", {"revision": current["revision"], "id": CTX_ID, "component": {"id": CTX_ID, "name": f"Redshift Parameters ({MODE})", "parameters": params}})
print("  JDBC driver linked to redshift_driver parameter")


section("Step 2: Create Process Group")

root_id = api("GET", "/process-groups/root")["id"]
pg = api("POST", f"/process-groups/{root_id}/process-groups", {
    "revision": {"version": 0},
    "component": {"name": f"Redshift to Snowflake [{MODE.upper()}]", "position": {"x": 500, "y": 500}},
})
PG_ID = pg["id"]
print(f"  Process Group: {PG_ID}")

pg_current = api("GET", f"/process-groups/{PG_ID}")
api("PUT", f"/process-groups/{PG_ID}", {
    "revision": pg_current["revision"], "id": PG_ID,
    "component": {"id": PG_ID, "parameterContext": {"id": CTX_ID}},
})
print("  Parameter context assigned")


section("Step 3: Create Controller Services")

dbcp = create_controller(PG_ID, "org.apache.nifi.dbcp.DBCPConnectionPool", "Redshift DBCP")
dbcp_rev = api("GET", f"/controller-services/{dbcp['id']}")["revision"]
api("PUT", f"/controller-services/{dbcp['id']}", {
    "revision": dbcp_rev, "id": dbcp["id"],
    "component": {"id": dbcp["id"], "properties": {
        "Database Connection URL": "#{redshift_url}",
        "Database Driver Class Name": "com.amazon.redshift.Driver",
        "database-driver-locations": "#{redshift_driver}",
        "Database User": "#{redshift_user}",
        "Password": "#{redshift_password}",
        "Max Total Connections": str(DBCP_MAX_CONNECTIONS),
        "Max Wait Time": "30000 millis",
        "Validation query": "SELECT 1",
    }},
})
print(f"  DBCP configured (max {DBCP_MAX_CONNECTIONS} connections)")

avro_reader = create_controller(PG_ID, "org.apache.nifi.avro.AvroReader", "Avro Reader")
json_reader = create_controller(PG_ID, "org.apache.nifi.json.JsonTreeReader", "JSON Reader")
json_writer = create_controller(PG_ID, "org.apache.nifi.json.JsonRecordSetWriter", "JSON Writer")

sf_conn = None
if MODE == "gold_mirror" and TRUNCATE_BEFORE_LOAD:
    sf_conn = create_controller(PG_ID, "com.snowflake.openflow.runtime.services.snowflake.SnowflakeConnectionService", "Snowflake Connection")
    sf_conn_rev = api("GET", f"/controller-services/{sf_conn['id']}")["revision"]
    api("PUT", f"/controller-services/{sf_conn['id']}", {
        "revision": sf_conn_rev, "id": sf_conn["id"],
        "component": {"id": sf_conn["id"], "properties": {
            "snowflake-database": "#{sf_database}",
            "snowflake-schema": "#{sf_schema}",
            "snowflake-warehouse": "#{sf_warehouse}",
            "snowflake-role": "#{sf_role}",
            "Auth Strategy": "SNOWFLAKE_SESSION_TOKEN",
        }},
    })
    print("  Snowflake Connection Service configured (for pre-load TRUNCATE)")


section("Step 4: Create Processors")

y = 100
ldt = create_processor(PG_ID, "ListDatabaseTables", "Discover Tables", 400, y)
configure_processor(ldt["id"], properties={
    "Database Connection Pooling Service": dbcp["id"],
    "Schema Pattern": REDSHIFT_SCHEMA_PATTERN,
    "Table Name Pattern": REDSHIFT_TABLE_PATTERN,
    "Table Types": "TABLE",
    "Include Count": "false",
    "Refresh Interval": "5 mins",
}, scheduling={"schedulingPeriod": "5 min", "schedulingStrategy": "TIMER_DRIVEN"})

y += 200
gtf_props = {
    "Database Connection Pooling Service": dbcp["id"],
    "Table Name": "${db.table.schema}.${db.table.name}",
    "Partition Size": GTF_PARTITION_SIZE,
}
if WATERMARK_COLUMN:
    gtf_props["Maximum-value Columns"] = WATERMARK_COLUMN
    print(f"  GTF: watermark={WATERMARK_COLUMN}, partition={GTF_PARTITION_SIZE}")
else:
    print(f"  GTF: no watermark (full extract), partition={GTF_PARTITION_SIZE}")

gtf = create_processor(PG_ID, "GenerateTableFetch", "Generate Queries", 400, y)
configure_processor(gtf["id"], properties=gtf_props, auto_terminate=["failure"])

y += 200
esql = create_processor(PG_ID, "org.apache.nifi.processors.standard.ExecuteSQL", "Execute Queries", 400, y)
configure_processor(esql["id"], properties={
    "Database Connection Pooling Service": dbcp["id"],
}, scheduling={"concurrentlySchedulableTaskCount": ESQL_CONCURRENT_TASKS}, auto_terminate=["failure"])

y += 200
convert = create_processor(PG_ID, "ConvertRecord", "Avro to JSON", 400, y)
configure_processor(convert["id"], properties={
    "Record Reader": avro_reader["id"],
    "Record Writer": json_writer["id"],
}, scheduling={"concurrentlySchedulableTaskCount": 5}, auto_terminate=["failure"])

# gold_mirror: insert TRUNCATE step between ConvertRecord and UpdateRecord
truncate_proc = None
if MODE == "gold_mirror" and TRUNCATE_BEFORE_LOAD and sf_conn:
    y += 200
    truncate_proc = create_processor(PG_ID, "ExecuteSQL", "Truncate Target Table", 400, y)
    # Uses Snowflake connection to truncate before load
    # The SQL runs: TRUNCATE TABLE IF EXISTS <schema>.<table>
    configure_processor(truncate_proc["id"], properties={
        "Database Connection Pooling Service": sf_conn["id"],
        "SQL select query": f"TRUNCATE TABLE IF EXISTS {SF_DATABASE}.{SF_SCHEMA}.${{db.table.name}}",
        "Content Output Strategy": "EMPTY",
    }, scheduling={"concurrentlySchedulableTaskCount": 3}, auto_terminate=["failure"])
    print(f"  TRUNCATE processor added (gold_mirror mode)")

y += 200
update = create_processor(PG_ID, "UpdateRecord", "Add Metadata", 400, y)
configure_processor(update["id"], properties={
    "Record Reader": json_reader["id"],
    "Record Writer": json_writer["id"],
    "Replacement Value Strategy": "literal-value",
    "/source_system": "REDSHIFT",
    "/source_table": "${db.table.name}",
    "/source_schema": "${db.table.schema}",
    "/replication_mode": MODE,
    "/ingested_at": '${now():format("yyyy-MM-dd HH:mm:ss")}',
}, scheduling={"concurrentlySchedulableTaskCount": 5}, auto_terminate=["failure"])

y += 200
pss = create_processor(PG_ID, "PutSnowpipeStreaming", "Write to Snowflake", 400, y)
configure_processor(pss["id"], properties={
    "Authentication Strategy": "SNOWFLAKE_SESSION_TOKEN",
    "Table": "${db.table.name}",
    "Database": SF_DATABASE,
    "Schema": SF_SCHEMA,
    "Role": SF_ROLE,
    "Record Reader": json_reader["id"],
    "Concurrency Group": "${db.table.name}",
    "Max Tasks Per Group": "3",
}, scheduling={"concurrentlySchedulableTaskCount": PSS_CONCURRENT_TASKS}, auto_terminate=["success", "failure"])


section("Step 5: Enable Controllers + Wire Connections")

controllers_to_enable = [dbcp, avro_reader, json_reader, json_writer]
if sf_conn:
    controllers_to_enable.append(sf_conn)
for cs in controllers_to_enable:
    enable_controller(cs["id"])
    time.sleep(1)
print("  All controllers enabled")

# Wire pipeline — LDT→GTF uses back-pressure to handle 1500-table queue bursts
create_connection(ldt["id"], gtf["id"], ["success"], PG_ID, backpressure=True)
create_connection(gtf["id"], esql["id"], ["success"], PG_ID, backpressure=True)
create_connection(esql["id"], convert["id"], ["success"], PG_ID)

if truncate_proc:
    create_connection(convert["id"], truncate_proc["id"], ["success"], PG_ID)
    create_connection(truncate_proc["id"], update["id"], ["success"], PG_ID)
else:
    create_connection(convert["id"], update["id"], ["success"], PG_ID)

create_connection(update["id"], pss["id"], ["success"], PG_ID)
print("  Full pipeline wired")


section("Step 6: Start Flow")

api("PUT", f"/flow/process-groups/{PG_ID}", {"id": PG_ID, "state": "RUNNING"})
print("  Flow started!")


section("SUMMARY")
watermark_info = f"watermark={WATERMARK_COLUMN}" if WATERMARK_COLUMN else "no watermark (full extract)"
truncate_info = "TRUNCATE before load" if TRUNCATE_BEFORE_LOAD and truncate_proc else "append-only"
print(f"""
  Mode: {MODE.upper()}
  Process Group:        {PG_ID}
  Parameter Context:    {CTX_ID}
  DBCP Pool:            {dbcp['id']} (max {DBCP_MAX_CONNECTIONS} connections)

  Pipeline:
    ListDatabaseTables  {ldt['id']}  schema={REDSHIFT_SCHEMA_PATTERN}, pattern={REDSHIFT_TABLE_PATTERN}
    GenerateTableFetch  {gtf['id']}  {watermark_info}, partition={GTF_PARTITION_SIZE}
    ExecuteSQL          {esql['id']}  {ESQL_CONCURRENT_TASKS} concurrent tasks
    ConvertRecord       {convert['id']}  Avro -> JSON
""" + (f"    Truncate Target     {truncate_proc['id']}  {truncate_info}\n" if truncate_proc else "") + f"""    UpdateRecord        {update['id']}  +source_system, +source_table, +replication_mode, +ingested_at
    PutSnowpipeStreaming {pss['id']}  Table=${{db.table.name}}, {PSS_CONCURRENT_TASKS} tasks

  Back-pressure (LDT->GTF, GTF->ExecuteSQL):
    Object threshold:   {NIFI_QUEUE_OBJECT_THRESHOLD}
    Size threshold:     {NIFI_QUEUE_SIZE_THRESHOLD}

  IMPORTANT: GTF Table Name uses ${{db.table.schema}}.${{db.table.name}}
  (LDT emits table name without schema prefix)

  Next steps:
    - Run setup/05_create_target_tables.py if not already done
""" + (f"    - Run setup/06_create_scd2_tables.py after first data load\n" if MODE == "scd2" else "") + """    - Monitor: python tests/parity_test.py
""")
