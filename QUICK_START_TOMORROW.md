# Quick Start Guide - Alta Analytics Project

## 🚀 Tomorrow's Checklist

### 1. Verify API Reset (should be after midnight)
```sql
-- Check recent loads in Snowflake
SELECT entity_name, office_id, last_run_utc, records_loaded
FROM RAW.REF.office_entity_watermark
WHERE office_id = 1
ORDER BY last_run_utc DESC;
```

### 2. Deploy the New Flows
```bash
cd "C:\Users\evanu\Documents\ETC Solutions\OctaneInsights\Prefect\altapest-prefect"
env\Scripts\activate
python -m flows.deploy_flows_snowflake
```

### 3. Test with Single Office First
```bash
# Run just office 1 to verify everything works
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct' -p test_office_id=1
```

### 4. Monitor the Run
- Check Prefect UI for flow status
- Monitor Snowflake for incoming data:
```sql
SELECT COUNT(*) as records_loaded, MAX(_loaded_at) as last_load
FROM RAW.fieldroutes.Customer_Dim
WHERE _loaded_at >= CURRENT_DATE();
```

### 5. If Successful, Run Full Load
```bash
# Remove test parameters to run all offices
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct'
```

## 📊 Next: Build Transformation Layer

### Create First Staging View
```sql
CREATE OR REPLACE VIEW STAGING.fieldroutes.v_customers AS
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
    RawData:balance::FLOAT as balance,
    RawData:isActive::BOOLEAN as is_active,
    RawData:dateAdded::TIMESTAMP_NTZ as date_added,
    RawData:dateUpdated::TIMESTAMP_NTZ as date_updated,
    _loaded_at
FROM RAW.fieldroutes.Customer_Dim
WHERE RawData IS NOT NULL;
```

## 🎯 End Goal This Week
1. ✅ Raw data pipeline (DONE!)
2. ⏳ Staging/transformation layer
3. ⏳ Analytics data model
4. ⏳ PowerBI dashboards
5. ⏳ User training

## 💡 Pro Tips
- API resets at midnight (timezone?)
- Each office has 20.5K daily API limit
- Run CDC during business hours only
- Monitor `RAW.REF.v_etl_monitoring` for issues

## 📝 For Claude Code Tomorrow
Say: "Continue Alta Pest Control project - ready to deploy flows and build transformation layer. See PROJECT_CONTEXT.md"

---
Good luck tomorrow! 🚀