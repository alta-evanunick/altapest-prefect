-- Check what loaded successfully from the recent run
-- Run this in Snowflake to see the results

-- Summary of recent loads
SELECT 
    entity_name,
    office_id,
    last_run_utc,
    records_loaded,
    DATEDIFF('minute', last_run_utc, CURRENT_TIMESTAMP()) as minutes_ago
FROM RAW.REF.office_entity_watermark
WHERE last_run_utc >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
ORDER BY last_run_utc DESC;

-- Count records loaded today by entity
SELECT 
    'Customer_Dim' as entity,
    COUNT(*) as records_loaded,
    COUNT(DISTINCT OfficeID) as offices_loaded,
    MAX(_loaded_at) as last_load_time
FROM RAW.fieldroutes.Customer_Dim
WHERE _loaded_at >= CURRENT_DATE()
UNION ALL
SELECT 'Employee_Dim', COUNT(*), COUNT(DISTINCT OfficeID), MAX(_loaded_at)
FROM RAW.fieldroutes.Employee_Dim
WHERE _loaded_at >= CURRENT_DATE()
UNION ALL
SELECT 'Appointment_Fact', COUNT(*), COUNT(DISTINCT OfficeID), MAX(_loaded_at)
FROM RAW.fieldroutes.Appointment_Fact
WHERE _loaded_at >= CURRENT_DATE()
UNION ALL
SELECT 'Ticket_Dim', COUNT(*), COUNT(DISTINCT OfficeID), MAX(_loaded_at)
FROM RAW.fieldroutes.Ticket_Dim
WHERE _loaded_at >= CURRENT_DATE()
UNION ALL
SELECT 'Payment_Fact', COUNT(*), COUNT(DISTINCT OfficeID), MAX(_loaded_at)
FROM RAW.fieldroutes.Payment_Fact
WHERE _loaded_at >= CURRENT_DATE()
ORDER BY entity;