-- =====================================================================
-- Standardize RAW_DB.FIELDROUTES Table Definitions
-- This script ensures all tables have consistent structure
-- =====================================================================

USE DATABASE RAW_DB;
USE SCHEMA FIELDROUTES;

-- First, let's check current table structures
-- This will help identify which tables need updating
SHOW TABLES;

-- Get detailed structure for each table (run this to see differences)
-- SELECT GET_DDL('TABLE', 'RAW_DB.FIELDROUTES.CUSTOMER_FACT');
-- SELECT GET_DDL('TABLE', 'RAW_DB.FIELDROUTES.TICKET_FACT');
-- etc...

-- =====================================================================
-- Option 1: Standardize to MINIMAL structure (recommended)
-- This removes constraints that could cause ETL failures
-- =====================================================================

-- Template for standardizing tables:
-- We'll remove PRIMARY KEY constraints and NOT NULL requirements
-- This is safer for ETL processes

-- Function to recreate tables with standard structure
CREATE OR REPLACE PROCEDURE STANDARDIZE_RAW_TABLE(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    SQL_STMT VARCHAR;
    TEMP_TABLE VARCHAR;
BEGIN
    TEMP_TABLE := TABLE_NAME || '_TEMP';
    
    -- Create temp table with standardized structure
    SQL_STMT := 'CREATE OR REPLACE TABLE ' || TEMP_TABLE || ' (
        OfficeID INTEGER,
        LoadDatetimeUTC TIMESTAMP_NTZ,
        RawData VARIANT
    )';
    EXECUTE IMMEDIATE SQL_STMT;
    
    -- Copy data from original table
    SQL_STMT := 'INSERT INTO ' || TEMP_TABLE || ' (OfficeID, LoadDatetimeUTC, RawData)
                 SELECT OfficeID, LoadDatetimeUTC, RawData FROM ' || TABLE_NAME;
    EXECUTE IMMEDIATE SQL_STMT;
    
    -- Drop original and rename temp
    SQL_STMT := 'DROP TABLE ' || TABLE_NAME;
    EXECUTE IMMEDIATE SQL_STMT;
    
    SQL_STMT := 'ALTER TABLE ' || TEMP_TABLE || ' RENAME TO ' || TABLE_NAME;
    EXECUTE IMMEDIATE SQL_STMT;
    
    RETURN 'Standardized table: ' || TABLE_NAME;
END;
$$;

-- List of all tables to standardize
-- Run these one by one and verify data after each
CALL STANDARDIZE_RAW_TABLE('OFFICE_DIM');
CALL STANDARDIZE_RAW_TABLE('REGION_DIM');
CALL STANDARDIZE_RAW_TABLE('SERVICETYPE_DIM');
CALL STANDARDIZE_RAW_TABLE('CUSTOMERSOURCE_DIM');
CALL STANDARDIZE_RAW_TABLE('GENERICFLAG_DIM');
CALL STANDARDIZE_RAW_TABLE('CANCELLATIONREASON_DIM');
CALL STANDARDIZE_RAW_TABLE('PRODUCT_DIM');
CALL STANDARDIZE_RAW_TABLE('RESERVICEREASON_DIM');
CALL STANDARDIZE_RAW_TABLE('CUSTOMER_FACT');
CALL STANDARDIZE_RAW_TABLE('EMPLOYEE_FACT');
CALL STANDARDIZE_RAW_TABLE('APPOINTMENT_FACT');
CALL STANDARDIZE_RAW_TABLE('SUBSCRIPTION_FACT');
CALL STANDARDIZE_RAW_TABLE('ROUTE_FACT');
CALL STANDARDIZE_RAW_TABLE('TICKET_FACT');
CALL STANDARDIZE_RAW_TABLE('TICKETITEM_FACT');
CALL STANDARDIZE_RAW_TABLE('PAYMENT_FACT');
CALL STANDARDIZE_RAW_TABLE('APPLIEDPAYMENT_FACT');
CALL STANDARDIZE_RAW_TABLE('NOTE_FACT');
CALL STANDARDIZE_RAW_TABLE('TASK_FACT');
CALL STANDARDIZE_RAW_TABLE('DOOR_FACT');
CALL STANDARDIZE_RAW_TABLE('DISBURSEMENT_FACT');
CALL STANDARDIZE_RAW_TABLE('CHARGEBACK_FACT');
CALL STANDARDIZE_RAW_TABLE('ADDITIONALCONTACTS_FACT');
CALL STANDARDIZE_RAW_TABLE('DISBURSEMENTITEM_FACT');
CALL STANDARDIZE_RAW_TABLE('GENERICFLAGASSIGNMENT_FACT');
CALL STANDARDIZE_RAW_TABLE('KNOCK_FACT');
CALL STANDARDIZE_RAW_TABLE('PAYMENTPROFILE_FACT');
CALL STANDARDIZE_RAW_TABLE('APPOINTMENTREMINDER_FACT');
CALL STANDARDIZE_RAW_TABLE('CUSTOMERCANCELLATIONREASONS_FACT');

-- Clean up the procedure
DROP PROCEDURE STANDARDIZE_RAW_TABLE;

-- =====================================================================
-- Option 2: Add indexes for performance (after standardizing)
-- =====================================================================

-- Add clustering for better performance (optional)
-- This doesn't enforce uniqueness but improves query performance
ALTER TABLE CUSTOMER_FACT CLUSTER BY (OfficeID, LoadDatetimeUTC);
ALTER TABLE TICKET_FACT CLUSTER BY (OfficeID, LoadDatetimeUTC);
ALTER TABLE PAYMENT_FACT CLUSTER BY (OfficeID, LoadDatetimeUTC);
ALTER TABLE APPOINTMENT_FACT CLUSTER BY (OfficeID, LoadDatetimeUTC);

-- =====================================================================
-- Verification Queries
-- =====================================================================

-- Check all table structures are now consistent
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'FIELDROUTES'
AND TABLE_CATALOG = 'RAW_DB'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- Check for any tables with extra columns like _LOADED_AT
SELECT DISTINCT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'FIELDROUTES'
AND TABLE_CATALOG = 'RAW_DB'
AND COLUMN_NAME NOT IN ('OFFICEID', 'LOADDATETIMEUTC', 'RAWDATA')
ORDER BY TABLE_NAME;

-- Check row counts to ensure no data loss
SELECT TABLE_NAME, ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'FIELDROUTES'
AND TABLE_CATALOG = 'RAW_DB'
AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;