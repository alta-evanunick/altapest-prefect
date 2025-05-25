-- ============================================
-- SNOWFLAKE SCHEMA SETUP FOR FIELDROUTES ETL
-- ============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW.fieldroutes;
CREATE SCHEMA IF NOT EXISTS RAW.REF;
CREATE SCHEMA IF NOT EXISTS STAGING.fieldroutes;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.fieldroutes;

-- ============================================
-- REFERENCE TABLES
-- ============================================

-- Office configuration table
CREATE OR REPLACE TABLE RAW.REF.offices_lookup (
    office_id               INT PRIMARY KEY,
    office_name             STRING,
    base_url                STRING,
    secret_block_name_key   STRING,
    secret_block_name_token STRING,
    active                  BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Watermark tracking table
CREATE OR REPLACE TABLE RAW.REF.office_entity_watermark (
    office_id        INT,
    entity_name      STRING,
    last_run_utc     TIMESTAMP_NTZ,
    last_success_utc TIMESTAMP_NTZ,
    records_loaded   INT DEFAULT 0,
    error_count      INT DEFAULT 0,
    PRIMARY KEY (office_id, entity_name)
);

-- Entity metadata reference
CREATE OR REPLACE TABLE RAW.REF.entity_metadata (
    entity_name      STRING PRIMARY KEY,
    table_name       STRING,
    is_dimension     BOOLEAN,
    is_small_entity  BOOLEAN,
    date_field       STRING,
    primary_key      STRING,
    load_frequency   STRING,
    active           BOOLEAN DEFAULT TRUE
);

-- ============================================
-- RAW DATA TABLES
-- ============================================

-- Dimension Tables
CREATE OR REPLACE TABLE RAW.fieldroutes.Customer_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Employee_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Office_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Region_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.ServiceType_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.CustomerSource_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.GenericFlag_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fact Tables
CREATE OR REPLACE TABLE RAW.fieldroutes.Appointment_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Subscription_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Route_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Ticket_Dim (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.TicketItem_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Payment_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.AppliedPayment_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Note_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.Task_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.AppointmentReminder_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.DoorKnock_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Combined table for financial transactions
CREATE OR REPLACE TABLE RAW.fieldroutes.FinancialTransaction_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    TransactionType  STRING,  -- 'disbursement' or 'chargeback'
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.fieldroutes.FlagAssignment_Fact (
    OfficeID         INT,
    LoadDatetimeUTC  TIMESTAMP_NTZ,
    RawData          VARIANT,
    _loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- STAGING VIEWS (for data transformation)
-- ============================================

-- Example staging view for customers
CREATE OR REPLACE VIEW STAGING.fieldroutes.v_customer AS
SELECT 
    OfficeID,
    LoadDatetimeUTC,
    RawData:customerID::INT as customer_id,
    RawData:firstName::STRING as first_name,
    RawData:lastName::STRING as last_name,
    RawData:email::STRING as email,
    RawData:phone::STRING as phone,
    RawData:address::STRING as address,
    RawData:city::STRING as city,
    RawData:state::STRING as state,
    RawData:zip::STRING as zip,
    RawData:active::BOOLEAN as is_active,
    RawData:dateAdded::TIMESTAMP_NTZ as date_added,
    RawData:dateUpdated::TIMESTAMP_NTZ as date_updated,
    _loaded_at
FROM RAW.fieldroutes.Customer_Dim
WHERE RawData IS NOT NULL;

-- ============================================
-- MONITORING VIEWS
-- ============================================

-- Load monitoring dashboard
CREATE OR REPLACE VIEW RAW.REF.v_etl_monitoring AS
SELECT 
    w.office_id,
    o.office_name,
    w.entity_name,
    w.last_run_utc,
    w.last_success_utc,
    w.records_loaded,
    w.error_count,
    DATEDIFF('hour', w.last_success_utc, CURRENT_TIMESTAMP()) as hours_since_last_success,
    CASE 
        WHEN DATEDIFF('hour', w.last_success_utc, CURRENT_TIMESTAMP()) > 24 THEN 'STALE'
        WHEN w.error_count > 3 THEN 'ERROR'
        ELSE 'OK'
    END as status
FROM RAW.REF.office_entity_watermark w
JOIN RAW.REF.offices_lookup o ON w.office_id = o.office_id
WHERE o.active = TRUE
ORDER BY status DESC, hours_since_last_success DESC;

-- ============================================
-- INITIAL DATA POPULATION
-- ============================================

-- Populate entity metadata
INSERT INTO RAW.REF.entity_metadata (entity_name, table_name, is_dimension, is_small_entity, date_field, primary_key, load_frequency)
VALUES
    ('customer', 'Customer_Dim', TRUE, FALSE, 'dateUpdated', 'customerID', 'nightly'),
    ('employee', 'Employee_Dim', TRUE, FALSE, 'dateUpdated', 'employeeID', 'nightly'),
    ('office', 'Office_Dim', TRUE, TRUE, 'dateAdded', 'officeID', 'nightly'),
    ('region', 'Region_Dim', TRUE, TRUE, 'dateAdded', 'regionID', 'nightly'),
    ('serviceType', 'ServiceType_Dim', TRUE, TRUE, 'dateAdded', 'serviceTypeID', 'nightly'),
    ('customerSource', 'CustomerSource_Dim', TRUE, TRUE, 'dateAdded', 'customerSourceID', 'nightly'),
    ('genericFlag', 'GenericFlag_Dim', TRUE, TRUE, 'dateAdded', 'genericFlagID', 'nightly'),
    ('appointment', 'Appointment_Fact', FALSE, FALSE, 'dateUpdated', 'appointmentID', 'cdc'),
    ('subscription', 'Subscription_Fact', FALSE, FALSE, 'dateUpdated', 'subscriptionID', 'cdc'),
    ('route', 'Route_Fact', FALSE, FALSE, 'dateUpdated', 'routeID', 'cdc'),
    ('ticket', 'Ticket_Dim', FALSE, FALSE, 'dateUpdated', 'ticketID', 'cdc'),
    ('ticketItem', 'TicketItem_Fact', FALSE, FALSE, 'dateUpdated', 'ticketItemID', 'cdc'),
    ('payment', 'Payment_Fact', FALSE, FALSE, 'dateUpdated', 'paymentID', 'cdc'),
    ('appliedPayment', 'AppliedPayment_Fact', FALSE, FALSE, 'dateUpdated', 'appliedPaymentID', 'nightly'),
    ('note', 'Note_Fact', FALSE, FALSE, 'dateAdded', 'noteID', 'nightly'),
    ('task', 'Task_Fact', FALSE, FALSE, 'dateAdded', 'taskID', 'nightly'),
    ('appointmentReminder', 'AppointmentReminder_Fact', FALSE, FALSE, 'dateUpdated', 'appointmentReminderID', 'nightly'),
    ('door', 'DoorKnock_Fact', FALSE, FALSE, 'dateUpdated', 'doorID', 'nightly'),
    ('disbursement', 'FinancialTransaction_Fact', FALSE, FALSE, 'dateUpdated', 'disbursementID', 'nightly'),
    ('chargeback', 'FinancialTransaction_Fact', FALSE, FALSE, 'dateUpdated', 'chargebackID', 'nightly'),
    ('flagAssignment', 'FlagAssignment_Fact', FALSE, FALSE, 'dateAdded', 'flagAssignmentID', 'nightly');

-- Initialize watermarks (run after offices are populated)
INSERT INTO RAW.REF.office_entity_watermark (office_id, entity_name, last_run_utc)
SELECT o.office_id, e.entity_name, DATEADD('day', -1, CURRENT_TIMESTAMP())
FROM RAW.REF.offices_lookup o
CROSS JOIN RAW.REF.entity_metadata e
WHERE o.active = TRUE AND e.active = TRUE;

-- ============================================
-- USEFUL QUERIES FOR OPERATIONS
-- ============================================

-- Check load status
-- SELECT * FROM RAW.REF.v_etl_monitoring;

-- Find failed loads
-- SELECT * FROM RAW.REF.office_entity_watermark 
-- WHERE error_count > 0 OR DATEDIFF('hour', last_success_utc, CURRENT_TIMESTAMP()) > 24;

-- Row counts by entity and office
-- SELECT 
--     REGEXP_SUBSTR(table_name, '[^.]+$') as entity,
--     OfficeID,
--     COUNT(*) as row_count,
--     MAX(_loaded_at) as last_loaded
-- FROM INFORMATION_SCHEMA.TABLES t
-- JOIN RAW.fieldroutes.* USING (table_name)
-- WHERE table_schema = 'FIELDROUTES' 
-- GROUP BY 1, 2
-- ORDER BY 1, 2;