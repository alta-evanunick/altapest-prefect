-- Fix table issues in Snowflake
USE DATABASE RAW;
USE SCHEMA FIELDROUTES;

-- Drop incorrectly created DIM tables (these should be FACT tables)
DROP TABLE IF EXISTS CUSTOMER_DIM;
DROP TABLE IF EXISTS EMPLOYEE_DIM;
DROP TABLE IF EXISTS TICKET_DIM;

-- Verify all required tables exist
-- Expected: 9 DIM tables, 21 FACT tables, 1 aggregation table = 31 main tables
-- Plus 30 staging tables

-- Check main tables
SELECT 'Main Tables:' as check_type;
SELECT COUNT(*) as table_count, 
       CASE 
         WHEN TABLE_NAME LIKE '%_DIM' THEN 'Dimension Tables'
         WHEN TABLE_NAME LIKE '%_FACT' THEN 'Fact Tables'
         ELSE 'Other Tables'
       END as table_type
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FIELDROUTES' 
  AND TABLE_NAME NOT LIKE '%_STAGING'
GROUP BY table_type
ORDER BY table_type;

-- List any missing staging tables
SELECT 'Missing Staging Tables:' as check_type;
WITH expected_staging AS (
    SELECT TABLE_NAME || '_STAGING' as staging_table_name
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'FIELDROUTES' 
      AND (TABLE_NAME LIKE '%_DIM' OR TABLE_NAME LIKE '%_FACT')
      AND TABLE_NAME NOT LIKE '%_STAGING'
      AND TABLE_NAME != 'CUSTOMERCANCELLATIONREASONS_FACT' -- This one doesn't need staging
)
SELECT staging_table_name
FROM expected_staging
WHERE staging_table_name NOT IN (
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'FIELDROUTES'
)
ORDER BY staging_table_name;

-- Create any missing staging tables
-- Run the create_missing_tables.sql script if staging tables are missing