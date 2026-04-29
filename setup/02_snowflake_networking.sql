-- =============================================================================
-- Snowflake Networking Setup for Redshift OpenFlow CDC
-- Run as ACCOUNTADMIN
--
-- BEFORE RUNNING: Edit the variables below to match your .env values
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────┐
-- │ EDIT THESE VALUES (must match your .env)                            │
-- └─────────────────────────────────────────────────────────────────────┘
SET NLB_DNS      = 'redshift-demo-nlb-e286b24201ac92e5.elb.us-west-2.amazonaws.com';  -- from 01_aws_redshift.sh
SET NLB_PORT     = '5439';
SET EAI_NAME     = 'REDSHIFT_OPENFLOW_EAI';
SET RUNTIME_ROLE = 'OPENFLOW_RUNTIME_ROLE_MYSQL';
-- ┌─────────────────────────────────────────────────────────────────────┐

-- Step 1: Create PrivateLink endpoint to the NLB
-- MANUAL: Snowsight > Admin > Accounts > PrivateLink > New
-- Use the VPC_ENDPOINT_SERVICE_NAME from 01_aws_redshift.sh output

-- Step 2: Create admin database for network rules
CREATE DATABASE IF NOT EXISTS ADMIN_DB;
CREATE SCHEMA IF NOT EXISTS ADMIN_DB.NETWORK_RULES;

-- Step 3: Network rule for Redshift access via PrivateLink
-- NOTE: VALUE_LIST does not support session variables, so this uses a literal.
--       Replace the DNS below if your NLB_DNS differs.
CREATE OR REPLACE NETWORK RULE ADMIN_DB.NETWORK_RULES.REDSHIFT_NR
    TYPE = PRIVATE_HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('redshift-demo-nlb-e286b24201ac92e5.elb.us-west-2.amazonaws.com:5439');
    -- ^^^ EDIT THIS to match your NLB_DNS:NLB_PORT from .env

-- Step 4: External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION REDSHIFT_OPENFLOW_EAI
    ALLOWED_NETWORK_RULES = (ADMIN_DB.NETWORK_RULES.REDSHIFT_NR)
    ENABLED = TRUE
    COMMENT = 'EAI for OpenFlow Redshift JDBC connectivity';

-- Step 5: Grant EAI to the OpenFlow runtime role
GRANT USAGE ON INTEGRATION REDSHIFT_OPENFLOW_EAI TO ROLE IDENTIFIER($RUNTIME_ROLE);

-- =============================================================================
-- MANUAL STEP: Attach EAI to OpenFlow runtime via Control Plane UI
-- 1. Navigate to OpenFlow in Snowsight
-- 2. Select your runtime
-- 3. Edit > External Access Integrations > Add REDSHIFT_OPENFLOW_EAI
-- =============================================================================
