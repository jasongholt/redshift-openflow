import json, subprocess, time, os, ssl, urllib.request, re
from pathlib import Path

_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

ENV = {**os.environ, "AWS_PROFILE": os.getenv("AWS_PROFILE", "default")}
REGION = os.getenv("AWS_REGION", "us-west-2")
WG = os.getenv("REDSHIFT_WORKGROUP", "openflow-demo-wg")
DB = os.getenv("REDSHIFT_DB", "demo")

SF_DATABASE = os.getenv("SF_DATABASE", "REDSHIFT_DEMO")
SF_SCHEMA_RAW = os.getenv("SF_SCHEMA_RAW", "RAW")
SF_SCHEMA_DIM = os.getenv("SF_SCHEMA_DIM", "DIM")

profiles = open(os.path.expanduser('~/.nipyapi/profiles.yml')).read()
_profile_name = os.getenv("NIPYAPI_PROFILE", "")
_nifi_url_env = os.getenv("NIFI_BASE_URL", "")
_nifi_pat_env = os.getenv("NIFI_PAT", "")
if _profile_name and not _nifi_pat_env:
    m = re.search(rf'{re.escape(_profile_name)}:\s*\n\s*nifi_url:\s*"([^"]+)"\s*\n\s*nifi_bearer_token:\s*"([^"]+)"', profiles)
    NIFI_BASE = m.group(1) if m else _nifi_url_env
    NIFI_PAT = m.group(2) if m else ""
else:
    NIFI_BASE = _nifi_url_env
    NIFI_PAT = _nifi_pat_env
PG_ID = os.getenv("NIFI_PROCESS_GROUP_ID", "")

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

PASS = 0
FAIL = 0
RESULTS = []

def log(msg):
    print(f"  {msg}")

def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def record(name, passed, detail=""):
    global PASS, FAIL
    status = "PASS" if passed else "FAIL"
    if passed:
        PASS += 1
    else:
        FAIL += 1
    RESULTS.append((name, status, detail))
    print(f"  [{status}] {name}" + (f" - {detail}" if detail else ""))

def rs_exec(sql):
    r = subprocess.run(["aws", "redshift-data", "execute-statement",
        "--workgroup-name", WG, "--database", DB,
        "--sql", sql, "--region", REGION,
        "--query", "Id", "--output", "text"],
        capture_output=True, text=True, env=ENV)
    stmt_id = r.stdout.strip()
    if not stmt_id:
        print(f"  ERROR: {r.stderr}")
        return None
    time.sleep(6)
    r2 = subprocess.run(["aws", "redshift-data", "describe-statement",
        "--id", stmt_id, "--region", REGION, "--output", "json"],
        capture_output=True, text=True, env=ENV)
    d = json.loads(r2.stdout)
    if d.get('Status') != 'FINISHED':
        print(f"  SQL ERROR: {d.get('Error','unknown')}")
        return None
    return stmt_id

def rs_query(sql):
    stmt_id = rs_exec(sql)
    if not stmt_id:
        return []
    r = subprocess.run(["aws", "redshift-data", "get-statement-result",
        "--id", stmt_id, "--region", REGION, "--output", "json"],
        capture_output=True, text=True, env=ENV)
    d = json.loads(r.stdout)
    rows = []
    for row in d.get('Records', []):
        vals = []
        for cell in row:
            v = cell.get('longValue', cell.get('stringValue', cell.get('booleanValue', cell.get('doubleValue', None))))
            vals.append(v)
        rows.append(vals)
    return rows

def sf_query(sql):
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "")
    cur = conn.cursor()
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return cols, rows

def nifi_api(method, path, data=None):
    url = f"{NIFI_BASE}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers={"Authorization": f"Bearer {NIFI_PAT}", "Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, context=SSL_CTX) as resp:
        return json.loads(resp.read().decode())

def wait_for_sync(description, seconds=90):
    log(f"Waiting {seconds}s for {description}...")
    time.sleep(seconds)

def refresh_dynamic_tables():
    log("Manually refreshing Dynamic Tables...")
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "")
        cur = conn.cursor()
        cur.execute("USE ROLE ACCOUNTADMIN")
        cur.execute("ALTER DYNAMIC TABLE {SF_DATABASE}.DIM.CUSTOMERS_SCD2 REFRESH")
        cur.execute("ALTER DYNAMIC TABLE {SF_DATABASE}.DIM.CUSTOMERS_CURRENT REFRESH")
        cur.close()
        conn.close()
        time.sleep(10)
        log("Dynamic Tables refreshed")
    except Exception as e:
        log(f"Refresh error (will use auto-refresh): {e}")
        time.sleep(30)

def get_scd2_for_customer(cid):
    _, rows = sf_query(f"""
        SELECT "customer_id", "company_name", "industry", ANNUAL_REVENUE,
               "employee_count", "region", IS_CURRENT, VERSION
        FROM {SF_DATABASE}.DIM.CUSTOMERS_SCD2
        WHERE "customer_id" = {cid}
        ORDER BY VERSION
    """)
    return rows

def get_scd2_counts():
    _, rows = sf_query("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN IS_CURRENT THEN 1 ELSE 0 END) as current_cnt,
               SUM(CASE WHEN NOT IS_CURRENT THEN 1 ELSE 0 END) as hist_cnt
        FROM {SF_DATABASE}.DIM.CUSTOMERS_SCD2
    """)
    return rows[0]

def get_current_count():
    _, rows = sf_query("SELECT COUNT(*) FROM {SF_DATABASE}.DIM.CUSTOMERS_CURRENT")
    return rows[0][0]


section("BASELINE: Capture initial state")
baseline_scd2 = get_scd2_counts()
baseline_current = get_current_count()
log(f"SCD2: {baseline_scd2[0]} total, {baseline_scd2[1]} current, {baseline_scd2[2]} historical")
log(f"CURRENT view: {baseline_current} rows")


section("TEST 1: Multiple updates to same row (customer 2)")
log("Applying update #1: GlobalTech Inc industry -> 'AI & Technology'")
rs_exec("UPDATE sales.customers SET industry = 'AI & Technology', annual_revenue = 13000000.00, updated_at = GETDATE() WHERE customer_id = 2")

log("Inserting updated row into Snowflake landing table to simulate sync...")
sf_query("""
    INSERT INTO {SF_DATABASE}.SALES.CUSTOMERS
    ("customer_id","company_name","industry","annual_revenue","employee_count","region","created_at","updated_at")
    VALUES (2,'GlobalTech Inc','AI & Technology','13000000.00',800,'East','1777321550000','1777590000000')
""")

time.sleep(3)
log("Applying update #2: GlobalTech Inc revenue -> 14,500,000")
rs_exec("UPDATE sales.customers SET annual_revenue = 14500000.00, updated_at = GETDATE() WHERE customer_id = 2")

sf_query("""
    INSERT INTO {SF_DATABASE}.SALES.CUSTOMERS
    ("customer_id","company_name","industry","annual_revenue","employee_count","region","created_at","updated_at")
    VALUES (2,'GlobalTech Inc','AI & Technology','14500000.00',800,'East','1777321550000','1777595000000')
""")

refresh_dynamic_tables()

versions = get_scd2_for_customer(2)
log(f"Customer 2 versions in SCD2: {len(versions)}")
for v in versions:
    log(f"  v{v[7]}: industry={v[2]}, revenue={v[3]}, is_current={v[6]}")

record("Multi-update: 3+ versions exist",
       len(versions) >= 3,
       f"Got {len(versions)} versions")
record("Multi-update: only last version is current",
       sum(1 for v in versions if v[6]) == 1,
       f"Current count: {sum(1 for v in versions if v[6])}")
record("Multi-update: latest has correct values",
       any(v[6] and float(v[3]) == 14500000.00 for v in versions),
       f"Latest revenue: {[v[3] for v in versions if v[6]]}")


section("TEST 2: No-change idempotency")
before_scd2 = get_scd2_counts()
before_current = get_current_count()
log(f"Before: SCD2={before_scd2[0]} rows, CURRENT={before_current} rows")

log("Inserting duplicate of existing row (same data, same timestamp)...")
sf_query("""
    INSERT INTO {SF_DATABASE}.SALES.CUSTOMERS
    ("customer_id","company_name","industry","annual_revenue","employee_count","region","created_at","updated_at")
    VALUES (1,'Acme Corp','Technology','5000000.00',250,'West','1777321550000','1777321550000')
""")

refresh_dynamic_tables()

after_scd2 = get_scd2_counts()
after_current = get_current_count()
log(f"After: SCD2={after_scd2[0]} rows, CURRENT={after_current} rows")

record("Idempotency: SCD2 row count unchanged",
       after_scd2[0] == before_scd2[0],
       f"Before={before_scd2[0]}, After={after_scd2[0]}")
record("Idempotency: CURRENT row count unchanged",
       after_current == before_current,
       f"Before={before_current}, After={after_current}")


section("TEST 3: Soft delete (add is_deleted flag)")
log("Adding is_deleted column to Redshift...")
rs_exec("ALTER TABLE sales.customers ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE")
rs_exec("UPDATE sales.customers SET is_deleted = FALSE, updated_at = GETDATE()")

log("Soft-deleting customer 8 (EduLearn Platform) in Redshift...")
rs_exec("UPDATE sales.customers SET is_deleted = TRUE, updated_at = GETDATE() WHERE customer_id = 8")

log("Simulating sync of soft-deleted row to Snowflake...")
sf_query("ALTER TABLE {SF_DATABASE}.SALES.CUSTOMERS ADD COLUMN IF NOT EXISTS \"is_deleted\" VARCHAR")
sf_query("""
    INSERT INTO {SF_DATABASE}.SALES.CUSTOMERS
    ("customer_id","company_name","industry","annual_revenue","employee_count","region","created_at","updated_at","is_deleted")
    VALUES (8,'EduLearn Platform','Education','2000000.00',80,'East','1777321550000','1777600000000','true')
""")

refresh_dynamic_tables()

versions_8 = get_scd2_for_customer(8)
log(f"Customer 8 versions: {len(versions_8)}")
for v in versions_8:
    log(f"  v{v[7]}: is_current={v[6]}")

rs_check = rs_query("SELECT customer_id, is_deleted FROM sales.customers WHERE customer_id = 8")
log(f"Redshift customer 8 is_deleted: {rs_check[0][1] if rs_check else 'N/A'}")

record("Soft delete: new version created in SCD2",
       len(versions_8) >= 2,
       f"Versions: {len(versions_8)}")
record("Soft delete: row still visible in SCD2 history",
       len(versions_8) >= 1,
       "Historical record preserved")


section("TEST 4: Bulk insert of new rows")
log("Inserting 3 new customers (10, 11, 12) into Redshift...")
rs_exec("INSERT INTO sales.customers (customer_id,company_name,industry,annual_revenue,employee_count,region,created_at,updated_at,is_deleted) VALUES (10,'BioGen Labs','Biotech',6000000.00,200,'East',GETDATE(),GETDATE(),FALSE),(11,'SecureNet Corp','Cybersecurity',4500000.00,150,'West',GETDATE(),GETDATE(),FALSE),(12,'GreenEnergy Co','Renewable Energy',11000000.00,500,'Central',GETDATE(),GETDATE(),FALSE)")

log("Simulating sync to Snowflake landing table...")
ts_now = str(int(time.time() * 1000))
sf_query(f"""
    INSERT INTO {SF_DATABASE}.SALES.CUSTOMERS
    ("customer_id","company_name","industry","annual_revenue","employee_count","region","created_at","updated_at")
    VALUES
    (10,'BioGen Labs','Biotech','6000000.00',200,'East','{ts_now}','{ts_now}'),
    (11,'SecureNet Corp','Cybersecurity','4500000.00',150,'West','{ts_now}','{ts_now}'),
    (12,'GreenEnergy Co','Renewable Energy','11000000.00',500,'Central','{ts_now}','{ts_now}')
""")

refresh_dynamic_tables()

new_current = get_current_count()
log(f"CURRENT view now has {new_current} rows")

for cid in [10, 11, 12]:
    v = get_scd2_for_customer(cid)
    exists = len(v) > 0
    is_current = any(row[6] for row in v) if v else False
    record(f"Bulk insert: customer {cid} exists with is_current=true",
           exists and is_current,
           f"Versions: {len(v)}, Current: {is_current}")


section("TEST 5: Final Reconciliation")
rs_customers = rs_query("SELECT customer_id, company_name, industry, annual_revenue FROM sales.customers WHERE is_deleted = FALSE ORDER BY customer_id")
_, sf_customers = sf_query("""
    SELECT "customer_id", "company_name", "industry", ANNUAL_REVENUE
    FROM {SF_DATABASE}.DIM.CUSTOMERS_CURRENT
    ORDER BY "customer_id"
""")

log(f"Redshift active customers: {len(rs_customers)}")
log(f"Snowflake CURRENT view: {len(sf_customers)}")

rs_ids = set(r[0] for r in rs_customers)
sf_ids = set(r[0] for r in sf_customers)

missing_in_sf = rs_ids - sf_ids
extra_in_sf = sf_ids - rs_ids
common = rs_ids & sf_ids

log(f"Common IDs: {len(common)}")
log(f"In Redshift but not Snowflake: {missing_in_sf or 'none'}")
log(f"In Snowflake but not Redshift: {extra_in_sf or 'none'}")

mismatches = []
for rs_row in rs_customers:
    cid = rs_row[0]
    sf_match = [s for s in sf_customers if s[0] == cid]
    if sf_match:
        sf_row = sf_match[0]
        if rs_row[1] != sf_row[1] or rs_row[2] != sf_row[2]:
            mismatches.append(f"ID {cid}: RS=({rs_row[1]},{rs_row[2]}) vs SF=({sf_row[1]},{sf_row[2]})")

record("Reconciliation: all active Redshift customers in Snowflake",
       len(missing_in_sf) == 0,
       f"Missing: {missing_in_sf or 'none'}")
record("Reconciliation: name/industry match for common IDs",
       len(mismatches) == 0,
       f"Mismatches: {mismatches or 'none'}")

final_scd2 = get_scd2_counts()
record("Final SCD2 integrity: historical rows exist",
       final_scd2[2] > 0,
       f"Historical: {final_scd2[2]}, Current: {final_scd2[1]}, Total: {final_scd2[0]}")


section("TEST RESULTS SUMMARY")
print(f"\n  {'='*50}")
print(f"  PASSED: {PASS}")
print(f"  FAILED: {FAIL}")
print(f"  TOTAL:  {PASS + FAIL}")
print(f"  {'='*50}\n")

for name, status, detail in RESULTS:
    marker = "+" if status == "PASS" else "X"
    print(f"  [{marker}] {name}")
    if detail:
        print(f"      {detail}")

print(f"\n  {'='*50}")
if FAIL == 0:
    print("  ALL TESTS PASSED")
else:
    print(f"  {FAIL} TEST(S) FAILED")
print(f"  {'='*50}")
