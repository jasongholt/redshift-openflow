#!/usr/bin/env python3
"""
Redshift-to-Snowflake OpenFlow Custom JDBC Connector
=====================================================

Builds a complete NiFi flow on an OpenFlow runtime that:
1. Discovers all tables in a Redshift schema via ListDatabaseTables
2. Generates paginated watermark queries via GenerateTableFetch
3. Executes queries in parallel via ExecuteSQL (10 concurrent tasks)
4. Converts Avro to JSON via ConvertRecord
5. Adds metadata (source_system, source_table, ingested_at) via UpdateRecord
6. Writes to Snowflake dynamically via PutSnowpipeStreaming (Table=${db.table.name})

Prerequisites:
  - OpenFlow runtime created with EAI attached (manual via Control Plane UI)
  - nipyapi installed and profile configured for the runtime
  - Redshift JDBC driver ZIP extracted to JDBC_JAR_DIR
  - Snowflake target tables pre-created (see setup/03_snowflake_objects.sql)

Usage:
  export NIPYAPI_PROFILE=jholt_admin_redshift  # or your profile name
  python build_flow.py
"""
import json
import glob
import os
import ssl
import time
import urllib.request
from pathlib import Path

env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# =============================================================================
# CONFIGURATION — Edit these values for your environment
# =============================================================================

NIFI_BASE_URL = os.getenv("NIFI_BASE_URL",
    f"https://{os.getenv('OPENFLOW_HOST', 'of--sfsenorthamerica-demojholtawsw.snowflakecomputing.app')}/{os.getenv('OPENFLOW_RUNTIME', 'redshft')}/nifi-api")

NIFI_PAT = os.getenv("NIFI_PAT", "")

JDBC_JAR_DIR = os.getenv("JDBC_JAR_DIR", "/tmp/redshift-jdbc")

REDSHIFT_URL = os.getenv("REDSHIFT_URL",
    f"jdbc:redshift://{os.getenv('NLB_DNS', 'localhost')}:{os.getenv('NLB_PORT', '5439')}/{os.getenv('REDSHIFT_DB', 'demo')}")
REDSHIFT_USER = os.getenv("REDSHIFT_ADMIN_USER", "admin")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_ADMIN_PASSWORD", "")

SF_DATABASE = os.getenv("SF_DATABASE", "REDSHIFT_DEMO")
SF_SCHEMA = os.getenv("SF_SCHEMA_RAW", os.getenv("SF_SCHEMA", "RAW"))
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "DEMO_JGH")
SF_ROLE = os.getenv("SF_RUNTIME_ROLE", "OPENFLOW_RUNTIME_ROLE_MYSQL")

REDSHIFT_SCHEMA_PATTERN = os.getenv("REDSHIFT_SCHEMA_PATTERN", os.getenv("REDSHIFT_SCHEMA", "sales"))
REDSHIFT_TABLE_PATTERN = os.getenv("REDSHIFT_TABLE_PATTERN", "%")
WATERMARK_COLUMN = os.getenv("WATERMARK_COLUMN", "updated_at")

DBCP_MAX_CONNECTIONS = int(os.getenv("DBCP_MAX_CONNECTIONS", "25"))
ESQL_CONCURRENT_TASKS = int(os.getenv("ESQL_CONCURRENT_TASKS", "10"))
PSS_CONCURRENT_TASKS = int(os.getenv("PSS_CONCURRENT_TASKS", "5"))

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

def create_connection(source_id, dest_id, rels, pg_id):
    body = {"revision": {"version": 0}, "component": {
        "source": {"id": source_id, "groupId": pg_id, "type": "PROCESSOR"},
        "destination": {"id": dest_id, "groupId": pg_id, "type": "PROCESSOR"},
        "selectedRelationships": rels,
    }}
    api("POST", f"/process-groups/{pg_id}/connections", body)
    print(f"  Connected [{', '.join(rels)}]")

def enable_controller(cs_id):
    current = api("GET", f"/controller-services/{cs_id}")
    api("PUT", f"/controller-services/{cs_id}/run-status", {
        "revision": current["revision"], "state": "ENABLED"
    })

def section(title):
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


# =============================================================================
# MAIN FLOW BUILD
# =============================================================================

if not NIFI_PAT:
    print("ERROR: Set NIFI_PAT environment variable with your NiFi bearer token")
    print("  Get it from: nipyapi profiles or OpenFlow session token")
    exit(1)

section("Step 1: Create Parameter Context + Upload JDBC Driver")

ctx_body = {
    "revision": {"version": 0},
    "component": {
        "name": "Redshift Parameters",
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
api("PUT", f"/parameter-contexts/{CTX_ID}", {"revision": current["revision"], "id": CTX_ID, "component": {"id": CTX_ID, "name": "Redshift Parameters", "parameters": params}})
print("  JDBC driver linked to redshift_driver parameter")


section("Step 2: Create Process Group")

root_id = api("GET", "/process-groups/root")["id"]
pg = api("POST", f"/process-groups/{root_id}/process-groups", {
    "revision": {"version": 0},
    "component": {"name": "Redshift to Snowflake CDC", "position": {"x": 500, "y": 500}},
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


section("Step 4: Create Processors")

ldt = create_processor(PG_ID, "ListDatabaseTables", "Discover Tables", 400, 100)
configure_processor(ldt["id"], properties={
    "Database Connection Pooling Service": dbcp["id"],
    "Schema Pattern": REDSHIFT_SCHEMA_PATTERN,
    "Table Name Pattern": REDSHIFT_TABLE_PATTERN,
    "Table Types": "TABLE",
    "Include Count": "false",
    "Refresh Interval": "5 mins",
}, scheduling={"schedulingPeriod": "5 min", "schedulingStrategy": "TIMER_DRIVEN"})

gtf = create_processor(PG_ID, "GenerateTableFetch", "Generate Queries", 400, 300)
configure_processor(gtf["id"], properties={
    "Database Connection Pooling Service": dbcp["id"],
    "Table Name": "${db.table.schema}.${db.table.name}",
    "Partition Size": "10000",
    "Maximum-value Columns": WATERMARK_COLUMN,
}, auto_terminate=["failure"])

esql = create_processor(PG_ID, "org.apache.nifi.processors.standard.ExecuteSQL", "Execute Queries", 400, 500)
configure_processor(esql["id"], properties={
    "Database Connection Pooling Service": dbcp["id"],
}, scheduling={"concurrentlySchedulableTaskCount": ESQL_CONCURRENT_TASKS}, auto_terminate=["failure"])

convert = create_processor(PG_ID, "ConvertRecord", "Avro to JSON", 400, 700)
configure_processor(convert["id"], properties={
    "Record Reader": avro_reader["id"],
    "Record Writer": json_writer["id"],
}, scheduling={"concurrentlySchedulableTaskCount": 5}, auto_terminate=["failure"])

update = create_processor(PG_ID, "UpdateRecord", "Add Metadata", 400, 900)
configure_processor(update["id"], properties={
    "Record Reader": json_reader["id"],
    "Record Writer": json_writer["id"],
    "Replacement Value Strategy": "literal-value",
    "/source_system": "REDSHIFT",
    "/source_table": "${db.table.name}",
    "/source_schema": "${db.table.schema}",
    '/ingested_at': '${now():format("yyyy-MM-dd HH:mm:ss")}',
}, scheduling={"concurrentlySchedulableTaskCount": 5}, auto_terminate=["failure"])

pss = create_processor(PG_ID, "PutSnowpipeStreaming", "Write to Snowflake", 400, 1100)
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

for cs in [dbcp, avro_reader, json_reader, json_writer]:
    enable_controller(cs["id"])
    time.sleep(1)
print("  All controllers enabled")

create_connection(ldt["id"], gtf["id"], ["success"], PG_ID)
create_connection(gtf["id"], esql["id"], ["success"], PG_ID)
create_connection(esql["id"], convert["id"], ["success"], PG_ID)
create_connection(convert["id"], update["id"], ["success"], PG_ID)
create_connection(update["id"], pss["id"], ["success"], PG_ID)
print("  Full pipeline wired")


section("Step 6: Start Flow")

api("PUT", f"/flow/process-groups/{PG_ID}", {"id": PG_ID, "state": "RUNNING"})
print("  Flow started!")


section("SUMMARY")
print(f"""
  Process Group:        {PG_ID}
  Parameter Context:    {CTX_ID}
  DBCP Pool:            {dbcp['id']} (max {DBCP_MAX_CONNECTIONS} connections)
  
  Pipeline:
    ListDatabaseTables  {ldt['id']}  schema={REDSHIFT_SCHEMA_PATTERN}, pattern={REDSHIFT_TABLE_PATTERN}
    GenerateTableFetch  {gtf['id']}  watermark={WATERMARK_COLUMN}, partition=10000
    ExecuteSQL          {esql['id']}  {ESQL_CONCURRENT_TASKS} concurrent tasks
    ConvertRecord       {convert['id']}  Avro -> JSON
    UpdateRecord        {update['id']}  +source_system, +source_table, +ingested_at
    PutSnowpipeStreaming {pss['id']}  Table=${{db.table.name}}, {PSS_CONCURRENT_TASKS} tasks

  IMPORTANT: GTF Table Name uses ${{db.table.schema}}.${{db.table.name}}
  (LDT emits table name without schema prefix)
""")
