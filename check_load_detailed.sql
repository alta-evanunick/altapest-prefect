-- Check what loaded successfully with more detail
-- Run these queries in Snowflake

-- 1. Check watermark table for recent loads
SELECT 
    entity_name,
    office_id,
    last_run_utc,
    records_loaded,
    last_success_utc,
    error_count
FROM RAW.REF.office_entity_watermark
WHERE office_id = 1
ORDER BY last_run_utc DESC;

-- 2. Check actual row counts (without date filter to see all data)
SELECT 'Customer_Dim' as table_name, COUNT(*) as total_rows, 
       MAX(_loaded_at) as most_recent_load,
       COUNT(DISTINCT LoadDatetimeUTC) as distinct_load_times
FROM RAW.fieldroutes.Customer_Dim WHERE OfficeID = 1
UNION ALL
SELECT 'Employee_Dim', COUNT(*), MAX(_loaded_at), COUNT(DISTINCT LoadDatetimeUTC)
FROM RAW.fieldroutes.Employee_Dim WHERE OfficeID = 1
UNION ALL
SELECT 'Appointment_Fact', COUNT(*), MAX(_loaded_at), COUNT(DISTINCT LoadDatetimeUTC)
FROM RAW.fieldroutes.Appointment_Fact WHERE OfficeID = 1
UNION ALL
SELECT 'Ticket_Dim', COUNT(*), MAX(_loaded_at), COUNT(DISTINCT LoadDatetimeUTC)
FROM RAW.fieldroutes.Ticket_Dim WHERE OfficeID = 1
UNION ALL
SELECT 'Payment_Fact', COUNT(*), MAX(_loaded_at), COUNT(DISTINCT LoadDatetimeUTC)
FROM RAW.fieldroutes.Payment_Fact WHERE OfficeID = 1;

-- 3. Check if data exists with the LoadDatetimeUTC from the run
-- Get the latest LoadDatetimeUTC first
WITH latest_load AS (
    SELECT MAX(last_run_utc) as load_time
    FROM RAW.REF.office_entity_watermark
    WHERE office_id = 1
)
SELECT 
    'Customer_Dim' as table_name,
    COUNT(*) as rows_from_latest_load
FROM RAW.fieldroutes.Customer_Dim c
CROSS JOIN latest_load l
WHERE c.OfficeID = 1 
AND c.LoadDatetimeUTC = l.load_time;

-- 4. Sample a few records to see the data structure
SELECT * FROM RAW.fieldroutes.Customer_Dim 
WHERE OfficeID = 1 
LIMIT 5;