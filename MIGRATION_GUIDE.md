# FieldRoutes to Snowflake Direct Load Migration Guide

## Overview

This migration removes the Azure Blob Storage intermediary step and loads data directly from the FieldRoutes API into Snowflake. This simplifies the pipeline and reduces potential points of failure.

## What Changed

### 1. Removed Azure Dependencies
- Removed `azure-storage-blob` and `prefect-azure` from requirements.txt
- No longer writing CSV files to Azure Blob Storage
- Direct JSON insertion into Snowflake tables

### 2. New Files Created
- `flows/fieldroutes_etl_flow_snowflake.py` - Main ETL flow with direct Snowflake loading
- `flows/fieldroutes_cdc_flow_snowflake.py` - CDC flow for high-velocity entities
- `flows/deploy_flows_snowflake.py` - Deployment script for the new flows
- `test_snowflake_direct.py` - Test script to validate the pipeline

### 3. Key Improvements
- **Faster Loading**: No intermediate file storage step
- **Better Error Handling**: Failed chunks don't prevent other data from loading
- **Efficient JSON Handling**: Using `orjson` for fast JSON serialization
- **Bulk Inserts**: Using Snowflake's `executemany` for efficient data loading
- **Automatic Deduplication**: Clears existing data for the same timestamp before inserting

## Migration Steps

### 1. Update Dependencies
```bash
pip install -r requirements.txt
```

### 2. Test the New Pipeline
```bash
# Test a single entity for a single office
python test_snowflake_direct.py --entity customer

# Test multiple entities
python test_snowflake_direct.py --full

# Test specific office
python test_snowflake_direct.py --office-id 123 --entity appointment
```

### 3. Deploy New Flows
```bash
# Create the Prefect deployments
python -m flows.deploy_flows_snowflake

# This creates two deployments:
# - fieldroutes-nightly-snowflake-direct (runs at 3 AM PT daily)
# - fieldroutes-cdc-snowflake-direct (runs every 2 hours during business hours)
```

### 4. Run Manual Tests
```bash
# Run nightly flow manually
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct'

# Run CDC flow manually
prefect deployment run 'FieldRoutes_CDC_ETL_Snowflake/fieldroutes-cdc-snowflake-direct'
```

## Data Structure

The data continues to be stored in the same Snowflake tables:
- Database: `RAW`
- Schema: `fieldroutes`
- Tables: Same table names (e.g., `Customer_Dim`, `Appointment_Fact`, etc.)

Each record contains:
- `OfficeID` - The office identifier
- `LoadDatetimeUTC` - When the data was loaded
- `RawData` - JSON object containing the full API response
- `_loaded_at` - Snowflake timestamp of when the row was inserted

## Monitoring

Check ETL status with:
```sql
-- View recent loads
SELECT * FROM RAW.REF.v_etl_monitoring
ORDER BY last_run_utc DESC;

-- Check specific entity loads
SELECT 
    OfficeID,
    COUNT(*) as record_count,
    MAX(LoadDatetimeUTC) as last_load,
    MAX(_loaded_at) as last_inserted
FROM RAW.fieldroutes.Customer_Dim
GROUP BY OfficeID
ORDER BY last_inserted DESC;
```

## Rollback Plan

If you need to revert to the Azure-based pipeline:
1. Use the original flow files: `fieldroutes_etl_flow.py` and `fieldroutes_cdc_flow.py`
2. Re-add Azure dependencies to requirements.txt
3. Deploy using the original `deploy_flows.py`

## Performance Considerations

- **API Rate Limits**: The pipeline includes pauses between offices to avoid overwhelming the API
- **Chunk Size**: Data is fetched in chunks of 1,000 records
- **Retry Logic**: Failed API calls are retried up to 3 times with exponential backoff
- **Partial Success**: If some entities fail, others will still be processed

## Troubleshooting

### Common Issues

1. **"Schema validation failed"**
   - Ensure all required tables exist in Snowflake
   - Run the SQL scripts in the `sql/` directory

2. **"No records loaded despite having IDs"**
   - Check API response structure in logs
   - Verify API credentials are correct

3. **"Snowflake load failed"**
   - Check Snowflake connection credentials
   - Verify user has INSERT permissions on the tables
   - Check for JSON parsing errors in the data

### Debug Mode

Enable verbose logging by running tests with specific entities:
```bash
python test_snowflake_direct.py --entity customer --office-id 1
```

This will show detailed API responses and Snowflake operations.