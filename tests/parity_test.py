import json, subprocess, time, os

ENV = {**os.environ, "AWS_PROFILE": "484577546576_Contributor"}
REGION = "us-west-2"
WG = "openflow-demo-wg"
DB = "demo"

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
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))

def rs_query(sql):
    r = subprocess.run(["aws", "redshift-data", "execute-statement",
        "--workgroup-name", WG, "--database", DB,
        "--sql", sql, "--region", REGION,
        "--query", "Id", "--output", "text"],
        capture_output=True, text=True, env=ENV)
    stmt_id = r.stdout.strip()
    if not stmt_id:
        print(f"  RS ERROR: {r.stderr}")
        return []
    for _ in range(15):
        time.sleep(2)
        r2 = subprocess.run(["aws", "redshift-data", "describe-statement",
            "--id", stmt_id, "--region", REGION, "--output", "json"],
            capture_output=True, text=True, env=ENV)
        d = json.loads(r2.stdout)
        if d.get('Status') == 'FINISHED':
            break
        if d.get('Status') == 'FAILED':
            print(f"  RS SQL ERROR: {d.get('Error','unknown')}")
            return []
    r3 = subprocess.run(["aws", "redshift-data", "get-statement-result",
        "--id", stmt_id, "--region", REGION, "--output", "json"],
        capture_output=True, text=True, env=ENV)
    d = json.loads(r3.stdout)
    rows = []
    for row in d.get('Records', []):
        vals = []
        for cell in row:
            if 'longValue' in cell: vals.append(cell['longValue'])
            elif 'stringValue' in cell: vals.append(cell['stringValue'])
            elif 'doubleValue' in cell: vals.append(cell['doubleValue'])
            elif 'booleanValue' in cell: vals.append(cell['booleanValue'])
            elif 'isNull' in cell: vals.append(None)
            else: vals.append(None)
        rows.append(vals)
    return rows

def sf_query(sql):
    import snowflake.connector
    conn = snowflake.connector.connect(connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "JGH-DEMO")
    cur = conn.cursor()
    cur.execute("USE ROLE ACCOUNTADMIN")
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return cols, rows


section("GROUP 1: Redshift vs Snowflake CURRENT Row Parity")

log("Querying Redshift (distinct active customers)...")
rs_rows = rs_query("""
    SELECT DISTINCT customer_id, company_name, industry, 
           annual_revenue, employee_count, region
    FROM sales.customers 
    WHERE is_deleted = FALSE
    ORDER BY customer_id
""")
rs_dict = {}
for r in rs_rows:
    cid = r[0]
    rs_dict[cid] = {
        'company_name': r[1],
        'industry': r[2],
        'annual_revenue': str(r[3]),
        'employee_count': r[4],
        'region': r[5],
    }
log(f"Redshift: {len(rs_dict)} distinct active customers: {sorted(rs_dict.keys())}")

log("Querying Snowflake CUSTOMERS_CURRENT...")
_, sf_rows = sf_query("""
    SELECT "customer_id", "company_name", "industry", 
           "ANNUAL_REVENUE", "employee_count", "region"
    FROM REDSHIFT_DEMO.DIM.CUSTOMERS_CURRENT
    ORDER BY "customer_id"
""")
sf_dict = {}
for r in sf_rows:
    cid = r[0]
    sf_dict[cid] = {
        'company_name': r[1],
        'industry': r[2],
        'annual_revenue': str(r[3]),
        'employee_count': r[4],
        'region': r[5],
    }
log(f"Snowflake CURRENT: {len(sf_dict)} rows: {sorted(sf_dict.keys())}")

rs_ids = set(rs_dict.keys())
sf_ids = set(sf_dict.keys())
missing_in_sf = rs_ids - sf_ids
extra_in_sf = sf_ids - rs_ids
common_ids = rs_ids & sf_ids

expected_sf_only = {9}
unexpected_sf_only = extra_in_sf - expected_sf_only

record("1.1 All Redshift active customers exist in Snowflake CURRENT",
       len(missing_in_sf) == 0,
       f"Missing in SF: {missing_in_sf or 'none'}")

record("1.2 Snowflake-only records are expected (customer 9 from earlier test)",
       len(unexpected_sf_only) == 0,
       f"Expected SF-only: {extra_in_sf & expected_sf_only or 'none'}, Unexpected: {unexpected_sf_only or 'none'}")

mismatches = []
for cid in sorted(common_ids):
    rs = rs_dict[cid]
    sf = sf_dict[cid]
    for col in ['company_name', 'industry', 'employee_count', 'region']:
        if str(rs[col]) != str(sf[col]):
            mismatches.append(f"ID {cid}.{col}: RS={rs[col]} vs SF={sf[col]}")
    rs_rev = float(rs['annual_revenue'])
    sf_rev = float(sf['annual_revenue'])
    if abs(rs_rev - sf_rev) > 0.01:
        mismatches.append(f"ID {cid}.annual_revenue: RS={rs_rev} vs SF={sf_rev}")

if mismatches:
    for m in mismatches:
        log(f"  MISMATCH: {m}")

record("1.3 Column values match for all common customers",
       len(mismatches) == 0,
       f"{len(mismatches)} mismatches" if mismatches else f"All {len(common_ids)} customers match perfectly")

log("\nSide-by-side (common customers):")
log(f"{'ID':>3}  {'Company':<22} {'Industry (RS)':<22} {'Industry (SF)':<22} {'Rev Match'}")
log("-" * 95)
for cid in sorted(common_ids):
    rs = rs_dict[cid]
    sf = sf_dict[cid]
    rev_ok = "OK" if abs(float(rs['annual_revenue']) - float(sf['annual_revenue'])) < 0.01 else "MISMATCH"
    log(f"{cid:>3}  {rs['company_name']:<22} {rs['industry']:<22} {sf['industry']:<22} {rev_ok}")


section("GROUP 2: SCD2 History Integrity")

log("Querying SCD2 Dynamic Table...")
_, scd2_rows = sf_query("""
    SELECT "customer_id", "company_name", "industry", "ANNUAL_REVENUE",
           "employee_count", "region", "IS_DELETED",
           "EFFECTIVE_FROM", "EFFECTIVE_TO", "IS_CURRENT", "VERSION"
    FROM REDSHIFT_DEMO.DIM.CUSTOMERS_SCD2
    ORDER BY "customer_id", "VERSION"
""")
log(f"SCD2 total rows: {len(scd2_rows)}")

from collections import defaultdict
by_customer = defaultdict(list)
for r in scd2_rows:
    by_customer[r[0]].append(r)

multi_current = []
for cid, versions in by_customer.items():
    current_count = sum(1 for v in versions if v[9] is True)
    if current_count != 1:
        multi_current.append(f"ID {cid}: {current_count} current versions")

record("2.1 Exactly one IS_CURRENT=true per customer",
       len(multi_current) == 0,
       f"Violations: {multi_current}" if multi_current else f"All {len(by_customer)} customers have exactly 1 current")

version_issues = []
for cid, versions in by_customer.items():
    expected_versions = list(range(1, len(versions) + 1))
    actual_versions = [v[10] for v in versions]
    if actual_versions != expected_versions:
        version_issues.append(f"ID {cid}: expected {expected_versions}, got {actual_versions}")

record("2.2 Version numbers are sequential (1, 2, 3...)",
       len(version_issues) == 0,
       f"Issues: {version_issues}" if version_issues else f"All {len(by_customer)} customers sequential")

sentinel_issues = []
for cid, versions in by_customer.items():
    last = versions[-1]
    eff_to = str(last[8])
    if '9999-12-31' not in eff_to:
        sentinel_issues.append(f"ID {cid}: last version EFFECTIVE_TO = {eff_to}")

record("2.3 Last version EFFECTIVE_TO = 9999-12-31",
       len(sentinel_issues) == 0,
       f"Issues: {sentinel_issues}" if sentinel_issues else "All sentinel dates correct")

gap_issues = []
for cid, versions in by_customer.items():
    if len(versions) < 2:
        continue
    for i in range(len(versions) - 1):
        eff_to_prev = versions[i][8]
        eff_from_next = versions[i+1][7]
        if str(eff_to_prev) != str(eff_from_next):
            gap_issues.append(f"ID {cid} v{versions[i][10]}->v{versions[i+1][10]}: {eff_to_prev} != {eff_from_next}")

record("2.4 No temporal gaps between versions",
       len(gap_issues) == 0,
       f"Gaps: {gap_issues}" if gap_issues else "All version chains continuous")

c2_versions = by_customer.get(2, [])
c2_ok = (len(c2_versions) >= 3
         and c2_versions[0][2] == 'Technology'
         and c2_versions[1][2] == 'AI & Technology'
         and any(float(v[3]) == 14500000.00 for v in c2_versions if v[9]))

record("2.5 Customer 2 has 3+ versions (Technology -> AI & Technology -> revenue bump)",
       c2_ok,
       f"Versions: {len(c2_versions)}, Industries: {[v[2] for v in c2_versions]}")

c4_versions = by_customer.get(4, [])
c4_ok = (len(c4_versions) >= 2
         and c4_versions[0][2] == 'Cloud Services'
         and c4_versions[-1][2] == 'Cloud Computing')

record("2.6 Customer 4 has 2+ versions (Cloud Services -> Cloud Computing)",
       c4_ok,
       f"Versions: {len(c4_versions)}, Industries: {[v[2] for v in c4_versions]}")

log("\nSCD2 Version Detail:")
for cid in sorted(by_customer.keys()):
    versions = by_customer[cid]
    if len(versions) > 1:
        log(f"  Customer {cid} ({versions[0][1]}): {len(versions)} versions")
        for v in versions:
            log(f"    v{v[10]}: industry={v[2]}, rev={v[3]}, current={v[9]}, from={v[7]}, to={v[8]}")


section("GROUP 3: Enriched Transformation Validation")

log("Querying CUSTOMERS_ENRICHED...")
_, enr_rows = sf_query("""
    SELECT CUSTOMER_ID, COMPANY_NAME, COMPANY_NAME_UPPER, INDUSTRY,
           ANNUAL_REVENUE, EMPLOYEE_COUNT, REGION, REGION_CODE,
           REVENUE_TIER, REVENUE_PER_EMPLOYEE, SOURCE_SYSTEM, INGESTED_AT
    FROM REDSHIFT_DEMO.RAW.CUSTOMERS_ENRICHED
""")
log(f"Enriched rows: {len(enr_rows)}")

upper_issues = []
for r in enr_rows:
    expected = r[1].upper() if r[1] else None
    actual = r[2]
    if expected != actual:
        upper_issues.append(f"ID {r[0]}: expected '{expected}', got '{actual}'")

record("3.1 company_name_upper == UPPER(company_name)",
       len(upper_issues) == 0,
       f"Issues: {upper_issues[:3]}" if upper_issues else f"All {len(enr_rows)} rows correct")

region_issues = []
for r in enr_rows:
    expected = r[6][0].upper() if r[6] else None
    actual = r[7]
    if expected != actual:
        region_issues.append(f"ID {r[0]}: region={r[6]}, expected code='{expected}', got='{actual}'")

record("3.2 region_code == first letter of UPPER(region)",
       len(region_issues) == 0,
       f"Issues: {region_issues[:3]}" if region_issues else f"All {len(enr_rows)} rows correct")

tier_issues = []
for r in enr_rows:
    rev = float(r[4]) if r[4] else 0
    if rev >= 10000000:
        expected = 'ENTERPRISE'
    elif rev >= 5000000:
        expected = 'MID_MARKET'
    elif rev >= 2000000:
        expected = 'COMMERCIAL'
    else:
        expected = 'STARTUP'
    actual = r[8]
    if expected != actual:
        tier_issues.append(f"ID {r[0]}: rev={rev}, expected={expected}, got={actual}")

record("3.3 revenue_tier thresholds correct",
       len(tier_issues) == 0,
       f"Issues: {tier_issues[:3]}" if tier_issues else f"All {len(enr_rows)} rows correct")

rpe_issues = []
for r in enr_rows:
    rev = float(r[4]) if r[4] else 0
    emp = int(r[5]) if r[5] else 0
    expected = rev / emp if emp > 0 else 0
    actual = float(r[9]) if r[9] else 0
    if abs(expected - actual) > 0.01:
        rpe_issues.append(f"ID {r[0]}: expected={expected:.2f}, got={actual:.2f}")

record("3.4 revenue_per_employee == annual_revenue / employee_count",
       len(rpe_issues) == 0,
       f"Issues: {rpe_issues[:3]}" if rpe_issues else f"All {len(enr_rows)} rows correct")

src_issues = [r[0] for r in enr_rows if r[10] != 'REDSHIFT']
record("3.5 source_system == 'REDSHIFT' for all rows",
       len(src_issues) == 0,
       f"Non-REDSHIFT IDs: {src_issues}" if src_issues else f"All {len(enr_rows)} rows = 'REDSHIFT'")

ts_issues = [r[0] for r in enr_rows if not r[11]]
record("3.6 ingested_at is populated for all rows",
       len(ts_issues) == 0,
       f"Null IDs: {ts_issues}" if ts_issues else f"All {len(enr_rows)} rows have timestamp")

log("\nSample enriched data:")
log(f"{'ID':>3}  {'Company':<22} {'Upper':<22} {'RC':<3} {'Tier':<12} {'RPE':>10}  {'Src':<10} {'Ingested'}")
log("-" * 100)
seen = set()
for r in sorted(enr_rows, key=lambda x: x[0]):
    if r[0] in seen:
        continue
    seen.add(r[0])
    log(f"{r[0]:>3}  {str(r[1]):<22} {str(r[2]):<22} {str(r[7]):<3} {str(r[8]):<12} {float(r[9]) if r[9] else 0:>10.0f}  {str(r[10]):<10} {str(r[11])}")


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
    print("  ALL TESTS PASSED - REDSHIFT == SNOWFLAKE PARITY CONFIRMED")
else:
    print(f"  {FAIL} TEST(S) FAILED")
print(f"  {'='*50}")
