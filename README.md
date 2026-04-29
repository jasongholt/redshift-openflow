# Redshift to Snowflake: Custom OpenFlow JDBC Connector with SCD Type 2

Replicate tables from Amazon Redshift Serverless into Snowflake via OpenFlow (hosted Apache NiFi on SPCS), with watermark-based incremental sync, in-flight metadata enrichment, and SCD Type 2 history tracking via Dynamic Tables.

## Architecture

```
Redshift Serverless
    |
    | (port 5439)
    v
Network Load Balancer (internal)
    |
    | AWS PrivateLink
    v
Snowflake VPC Endpoint
    |
    v
OpenFlow Runtime (NiFi on SPCS)
    |
    | ListDatabaseTables    -- discovers all tables in schema
    | GenerateTableFetch    -- generates paginated watermark queries
    | ExecuteSQL            -- executes queries (10 concurrent tasks)
    | ConvertRecord         -- Avro -> JSON
    | UpdateRecord          -- adds source_system, source_table, ingested_at
    | PutSnowpipeStreaming  -- writes to Snowflake (dynamic table routing)
    |
    v
REDSHIFT_DEMO.RAW.*         -- landing tables (one per source table)
    |
    v (Dynamic Table, 5-min lag)
REDSHIFT_DEMO.DIM.CUSTOMERS_SCD2    -- SCD Type 2 (full history)
    |
    v (Dynamic Table, 5-min lag)
REDSHIFT_DEMO.DIM.CUSTOMERS_CURRENT -- latest version per customer
```

## Prerequisites

- AWS account with permissions for Redshift Serverless, EC2 (NLB, VPC endpoints), IAM
- Snowflake account with ACCOUNTADMIN and an OpenFlow deployment
- Python 3.10+ with `nipyapi` installed (for tests: also `snowflake-connector-python`)
- AWS CLI configured

## Setup Steps

### Step 1: AWS Infrastructure

Creates Redshift Serverless, NLB, and VPC Endpoint Service for PrivateLink.

```bash
chmod +x setup/01_aws_redshift.sh
./setup/01_aws_redshift.sh
```

Save the output values: `NLB_DNS` and `VPC_ENDPOINT_SERVICE_NAME`.

### Step 2: Snowflake Networking (MANUAL + SQL)

1. **Create PrivateLink endpoint** (Snowsight: Admin > Accounts > PrivateLink > New, or via support)
2. Edit `setup/02_snowflake_networking.sql` — replace `<NLB_DNS>` with your NLB DNS
3. Run the SQL as ACCOUNTADMIN:

```sql
-- In Snowsight or SnowSQL
!source setup/02_snowflake_networking.sql
```

### Step 3: Create OpenFlow Runtime (MANUAL)

This step cannot be automated via API.

1. Navigate to **Snowsight > Data Integration > OpenFlow**
2. Click **Create Runtime**
3. Name: `redshift` (or your choice)
4. Size: **Medium** for dev, **Large** for production
5. Max Nodes: **3** (auto-scales)
6. **Attach EAI**: Select `REDSHIFT_OPENFLOW_EAI`
7. Wait for runtime to reach **ACTIVE** status

### Step 4: Snowflake Objects

Creates the database, schemas, target tables, and SCD2 Dynamic Tables.

```sql
-- Edit warehouse name in the Dynamic Table DDL if yours differs from DEMO_JGH
!source setup/03_snowflake_objects.sql
```

### Step 5: Seed Redshift

```bash
# Via AWS Data API
aws redshift-data execute-statement \
    --workgroup-name openflow-demo-wg \
    --database demo \
    --sql "$(cat setup/04_seed_redshift.sql)" \
    --region us-west-2
```

Or connect to Redshift via any SQL client and run `setup/04_seed_redshift.sql`.

### Step 6: Extract JDBC Driver

```bash
mkdir -p /tmp/redshift-jdbc
cd /tmp/redshift-jdbc
unzip /path/to/drivers/redshift-jdbc42-2.2.5.zip
```

### Step 7: Build the NiFi Flow

Configure your nipyapi profile for the runtime, then:

```bash
export NIFI_PAT="<your-nifi-bearer-token>"
export NIFI_BASE_URL="https://of--<account>.snowflakecomputing.app/<runtime>/nifi-api"
export JDBC_JAR_DIR="/tmp/redshift-jdbc"
export REDSHIFT_PASSWORD="OpenFlowDemo2026"

python connector/build_flow.py
```

The script creates the entire pipeline: parameter context, JDBC driver upload, controllers, 6 processors, and all connections.

### Step 8: Validate

```bash
# Full parity test: Redshift == Snowflake (15 tests)
SNOWFLAKE_CONNECTION_NAME=<your-connection> python tests/parity_test.py

# SCD2 mutation tests (13 tests)
SNOWFLAKE_CONNECTION_NAME=<your-connection> python tests/scd2_test_suite.py
```

## What Each File Does

| File | Purpose |
|---|---|
| `setup/01_aws_redshift.sh` | AWS: Redshift Serverless + NLB + VPC Endpoint Service |
| `setup/02_snowflake_networking.sql` | Snowflake: Network Rule, EAI, PrivateLink |
| `setup/03_snowflake_objects.sql` | Snowflake: Database, schemas, tables, Dynamic Tables (SCD2) |
| `setup/04_seed_redshift.sql` | Redshift: Sample data (customers, orders, products) |
| `connector/build_flow.py` | NiFi: Complete flow builder (LDT->GTF->ESQL->Convert->Update->PSS) |
| `tests/parity_test.py` | Validates Redshift == Snowflake data parity (15 tests) |
| `tests/scd2_test_suite.py` | Tests SCD2 behavior: multi-update, idempotency, soft delete (13 tests) |
| `drivers/redshift-jdbc42-2.2.5.zip` | Redshift JDBC driver v2.2.5 (56 JARs) |
| `docs/architecture.drawio` | Architecture diagram (open with draw.io) |

## Manual Steps (Cannot Be Scripted)

| Step | Why |
|---|---|
| Create OpenFlow runtime | No API, must use Control Plane UI |
| Attach EAI to runtime | No API, must use Control Plane UI |
| Create PrivateLink endpoint | Requires Snowflake admin action |

## Key Lessons Learned

- **GTF Table Name must be schema-qualified**: `${db.table.schema}.${db.table.name}` because ListDatabaseTables emits table name without schema prefix
- **nipyapi `create_parameter_context()` returns 500** on some runtimes — use raw NiFi REST API instead
- **PutSnowpipeStreaming target tables must exist** before data flows — pre-create with grants
- **UpdateRecord literal-value strategy** cannot reference other fields via `${field.value}` — use QueryRecord SQL for cross-field transforms
- **NiFi ProcessorsApi `clear_state`** is version-suffixed as `clear_state3()` in nipyapi
- **Redshift password special characters** (`!`, `@`) can cause issues — use alphanumeric passwords

## Scaling to 1200+ Tables

The flow uses ListDatabaseTables + GenerateTableFetch, which handles any number of tables with a fixed set of 6 processors. To scale:

1. **Runtime**: Upgrade to Large (8 vCPU, 20 GB), max 3-5 nodes
2. **DBCP Pool**: Already set to 25 connections
3. **ExecuteSQL**: Already set to 10 concurrent tasks
4. **Target tables**: Must be pre-created in Snowflake for each source table
5. **Back pressure**: Increase queue thresholds if needed (default 10,000 FlowFiles)
