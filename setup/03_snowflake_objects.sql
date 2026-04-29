-- =============================================================================
-- Snowflake Objects for Redshift OpenFlow Multi-Mode Replication
-- Run as ACCOUNTADMIN
--
-- BEFORE RUNNING: Edit the SET variables below to match your .env
--
-- This script handles:
--   - Database and schema creation
--   - Role grants to the OpenFlow runtime role
--
-- For target table DDL (1500 tables), use:
--   python setup/05_create_target_tables.py
--
-- For SCD2 Dynamic Tables (scd2 mode only), use AFTER first data load:
--   python setup/06_create_scd2_tables.py
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────┐
-- │ EDIT THESE VALUES (must match your .env)                            │
-- └─────────────────────────────────────────────────────────────────────┘
SET DB       = 'REDSHIFT_DEMO';
SET WH       = 'DEMO_JGH';
SET RT_ROLE  = 'OPENFLOW_RUNTIME_ROLE_MYSQL';
-- └─────────────────────────────────────────────────────────────────────┘

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
GRANT USAGE ON SCHEMA IDENTIFIER($DB || '.DIM') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON ALL TABLES IN SCHEMA IDENTIFIER($DB || '.SALES') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON ALL TABLES IN SCHEMA IDENTIFIER($DB || '.RAW') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON FUTURE TABLES IN SCHEMA IDENTIFIER($DB || '.SALES') TO ROLE IDENTIFIER($RT_ROLE);
GRANT ALL ON FUTURE TABLES IN SCHEMA IDENTIFIER($DB || '.RAW') TO ROLE IDENTIFIER($RT_ROLE);
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA IDENTIFIER($DB || '.DIM') TO ROLE IDENTIFIER($RT_ROLE);
GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA IDENTIFIER($DB || '.DIM') TO ROLE IDENTIFIER($RT_ROLE);

-- =============================================================================
-- Step 3: Create target tables and SCD2 Dynamic Tables via Python
-- =============================================================================

-- Run these after completing the SQL steps above:
--
--   # Create RAW target tables (all source tables, any mode):
--   python setup/05_create_target_tables.py
--
--   # Build the NiFi flow:
--   python connector/build_flow.py
--
--   # After first data load — create SCD2 Dynamic Tables (scd2 mode only):
--   python setup/06_create_scd2_tables.py
