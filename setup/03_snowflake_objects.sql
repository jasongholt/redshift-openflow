-- =============================================================================
-- Snowflake Objects for Redshift CDC SCD2 Demo
-- Run as ACCOUNTADMIN
--
-- BEFORE RUNNING: Edit the variables below to match your .env values
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────┐
-- │ EDIT THESE VALUES (must match your .env)                            │
-- └─────────────────────────────────────────────────────────────────────┘
SET DB       = 'REDSHIFT_DEMO';
SET WH       = 'DEMO_JGH';
SET RT_ROLE  = 'OPENFLOW_RUNTIME_ROLE_MYSQL';
-- ┌─────────────────────────────────────────────────────────────────────┘

-- =============================================================================
-- Step 1: Create database and schemas
-- =============================================================================
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($DB || '.SALES');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($DB || '.RAW');
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($DB || '.DIM');

-- =============================================================================
-- Step 2: Grant access to OpenFlow runtime role
-- =============================================================================
GRANT USAGE ON DATABASE IDENTIFIER($DB) TO ROLE IDENTIFIER($RT_ROLE);
GRANT USAGE ON SCHEMA IDENTIFIER($DB || '.SALES') TO ROLE IDENTIFIER($RT_ROLE);
GRANT USAGE ON SCHEMA IDENTIFIER($DB || '.RAW') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON ALL TABLES IN SCHEMA IDENTIFIER($DB || '.SALES') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON ALL TABLES IN SCHEMA IDENTIFIER($DB || '.RAW') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON FUTURE TABLES IN SCHEMA IDENTIFIER($DB || '.SALES') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON FUTURE TABLES IN SCHEMA IDENTIFIER($DB || '.RAW') TO ROLE IDENTIFIER($RT_ROLE);

-- =============================================================================
-- Step 3: Create RAW target tables (must exist before PutSnowpipeStreaming writes)
-- =============================================================================
USE SCHEMA IDENTIFIER($DB || '.RAW');

CREATE TABLE IF NOT EXISTS CUSTOMERS (
    customer_id NUMBER, company_name VARCHAR, industry VARCHAR,
    annual_revenue NUMBER(15,2), employee_count NUMBER, region VARCHAR,
    created_at VARCHAR, updated_at VARCHAR, is_deleted VARCHAR,
    source_system VARCHAR, source_table VARCHAR, source_schema VARCHAR, ingested_at VARCHAR
);

CREATE TABLE IF NOT EXISTS ORDERS (
    order_id NUMBER, customer_id NUMBER, product_name VARCHAR,
    quantity NUMBER, unit_price NUMBER(15,2), total_amount NUMBER(15,2),
    order_status VARCHAR, order_date VARCHAR, updated_at VARCHAR,
    source_system VARCHAR, source_table VARCHAR, source_schema VARCHAR, ingested_at VARCHAR
);

CREATE TABLE IF NOT EXISTS PRODUCTS (
    product_id NUMBER, product_name VARCHAR, category VARCHAR,
    list_price NUMBER(15,2), cost_price NUMBER(15,2), supplier VARCHAR,
    in_stock VARCHAR, updated_at VARCHAR,
    source_system VARCHAR, source_table VARCHAR, source_schema VARCHAR, ingested_at VARCHAR
);

-- =============================================================================
-- Step 4: Enable change tracking on landing table (required for Dynamic Tables)
-- NOTE: Run this AFTER data has been loaded via OpenFlow at least once
-- =============================================================================
USE SCHEMA IDENTIFIER($DB || '.SALES');
-- ALTER TABLE CUSTOMERS SET CHANGE_TRACKING = ON;
-- (Uncomment after first data load creates the table)

-- =============================================================================
-- Step 5: SCD Type 2 Dynamic Table
-- Deduplicates, tracks versions with effective_from/effective_to, marks current
-- =============================================================================
USE SCHEMA IDENTIFIER($DB || '.DIM');

CREATE OR REPLACE DYNAMIC TABLE CUSTOMERS_SCD2
    TARGET_LAG = '5 minutes'
    WAREHOUSE = IDENTIFIER($WH)
AS
WITH source_data AS (
    SELECT
        "customer_id", "company_name", "industry",
        "annual_revenue", "employee_count", "region",
        COALESCE("updated_at", "created_at") AS version_ts,
        COALESCE("is_deleted", 'false') AS is_deleted,
        "company_name" || '|' || "industry" || '|' || "annual_revenue" || '|' ||
        "employee_count" || '|' || "region" || '|' || COALESCE("is_deleted", 'false') AS row_hash
    FROM IDENTIFIER($DB || '.SALES.CUSTOMERS')
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY "customer_id", row_hash
            ORDER BY version_ts
        ) AS dup_rank
    FROM source_data
),
unique_versions AS (
    SELECT "customer_id", "company_name", "industry",
           "annual_revenue", "employee_count", "region", version_ts, is_deleted
    FROM deduped WHERE dup_rank = 1
),
versioned AS (
    SELECT
        "customer_id", "company_name", "industry",
        "annual_revenue"::NUMBER(15,2) AS annual_revenue,
        "employee_count", "region",
        is_deleted::BOOLEAN AS is_deleted,
        TO_TIMESTAMP(version_ts::NUMBER / 1000) AS effective_from,
        LEAD(TO_TIMESTAMP(version_ts::NUMBER / 1000)) OVER (
            PARTITION BY "customer_id" ORDER BY version_ts
        ) AS next_version_ts,
        ROW_NUMBER() OVER (
            PARTITION BY "customer_id" ORDER BY version_ts DESC
        ) AS rev_rank,
        ROW_NUMBER() OVER (
            PARTITION BY "customer_id" ORDER BY version_ts ASC
        ) AS version
    FROM unique_versions
)
SELECT
    "customer_id", "company_name", "industry",
    ANNUAL_REVENUE, "employee_count", "region",
    IS_DELETED,
    EFFECTIVE_FROM,
    COALESCE(NEXT_VERSION_TS, '9999-12-31'::TIMESTAMP) AS effective_to,
    CASE WHEN REV_RANK = 1 THEN TRUE ELSE FALSE END AS is_current,
    VERSION
FROM versioned;

-- =============================================================================
-- Step 6: Current-state view (latest version per customer, excludes soft deletes)
-- =============================================================================
CREATE OR REPLACE DYNAMIC TABLE CUSTOMERS_CURRENT
    TARGET_LAG = '5 minutes'
    WAREHOUSE = IDENTIFIER($WH)
AS
SELECT
    "customer_id", "company_name", "industry",
    ANNUAL_REVENUE, "employee_count", "region",
    EFFECTIVE_FROM AS last_updated
FROM CUSTOMERS_SCD2
WHERE IS_CURRENT = TRUE
  AND IS_DELETED = FALSE;
