# Testing Instructions for Direct Snowflake Load

## Overview
We've implemented a direct FieldRoutes API to Snowflake pipeline that bypasses Azure Blob Storage. Before proceeding with the full deployment, we need to test with minimal API calls.

## Test Files Created

### 1. `test_direct_minimal.py`
A minimal test script that:
- Tests ONLY the customer entity
- Tests ONLY office_id=1
- Fetches data from the last 7 days
- Limits the fetch to 10 customers to minimize API calls
- Tests a single Snowflake insert

### 2. `test_snowflake_direct.py`
A comprehensive test script for full testing (use after minimal test passes)

## How to Run the Test

### Option 1: From Windows Command Prompt
```cmd
cd "C:\Users\evanu\Documents\ETC Solutions\OctaneInsights\Prefect\altapest-prefect"
env\Scripts\activate
python test_direct_minimal.py
```

### Option 2: Using the batch file
Double-click `run_test.bat` in Windows Explorer

### Option 3: From WSL (if dependencies are installed)
```bash
cd "/mnt/c/Users/evanu/Documents/ETC Solutions/OctaneInsights/Prefect/altapest-prefect"
./env/Scripts/python.exe test_direct_minimal.py
```

## Expected Output

The test will:
1. Connect to Snowflake and retrieve office 1 configuration
2. Call the FieldRoutes API to search for customers updated in the last 7 days
3. Fetch details for up to 10 customers
4. Insert 1 test record into Snowflake
5. Verify the insert worked

You should see output like:
```
2025-05-27 15:00:00,000 - INFO - Testing customer entity for office_id=1
2025-05-27 15:00:01,000 - INFO - Found office: 1 - [Office Name]
2025-05-27 15:00:02,000 - INFO - Found 150 customer IDs in field 'customerIDs'
2025-05-27 15:00:03,000 - INFO - Retrieved 10 customer records
2025-05-27 15:00:04,000 - INFO - Snowflake insert test: 1 rows inserted
2025-05-27 15:00:04,000 - INFO - ✅ Test completed successfully!
```

## API Call Count

This minimal test makes only 2 API calls:
1. One search call to get customer IDs
2. One get call to fetch 10 customer records

Total API calls: 2 (well within the 20.5K daily limit)

## Next Steps

After the minimal test passes:

1. **Test more entities** (one at a time):
   ```
   python test_snowflake_direct.py --office-id 1 --entity office
   python test_snowflake_direct.py --office-id 1 --entity appointment
   ```

2. **Deploy the flows**:
   ```
   python -m flows.deploy_flows_snowflake
   ```

3. **Run a manual flow test**:
   ```
   prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct' -p test_office_id=1 -p test_entity=customer
   ```

## Monitoring API Usage

To check how many records were fetched (and estimate API calls):
```sql
SELECT 
    entity_name,
    office_id,
    last_run_utc,
    records_loaded,
    error_count
FROM RAW.REF.office_entity_watermark
WHERE office_id = 1
ORDER BY last_run_utc DESC;
```

## Troubleshooting

If the test fails:

1. **Check Snowflake connectivity**: The error will indicate if it's a connection issue
2. **Check API credentials**: Look for 401/403 errors in the output
3. **Check API response format**: The test logs the full search response structure
4. **Check Snowflake permissions**: Look for permission errors on INSERT

## Important Notes

- The new pipeline is in separate files (*_snowflake.py) so the old Azure pipeline still works
- No changes to the existing flows until we confirm the new approach works
- All data goes into the same Snowflake tables as before