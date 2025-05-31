# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## IMPORTANT CODING STANDARDS

**UTF-8 Compatibility**: Do NOT use the red "X" emoji (❌) in any code files - it causes UTF-8 deployment errors. Use text alternatives like "FAILED", "ERROR", or "[X]" instead.

## Project Overview

This is a Prefect-based ETL pipeline that extracts data from the FieldRoutes API and loads it directly into Snowflake. The pipeline handles both nightly full loads and CDC (Change Data Capture) for high-velocity entities.

### Key Architecture Components

1. **Direct to Snowflake Loading**: Data flows directly from FieldRoutes API → Snowflake (bypassing Azure Blob Storage)
2. **Two Main Flows**:
   - **Nightly ETL**: Full daily extract for all entities (runs at 3 AM PT)
   - **CDC ETL**: Incremental updates for high-velocity entities (runs every 2 hours during business hours)
3. **Entity-Based Processing**: 40+ entity types with different characteristics (dimensions vs facts, small vs large)
4. **Office-Based Multi-tenancy**: Each office has its own API credentials and data is segregated by office_id

### Data Flow Architecture

```
FieldRoutes API → Prefect Tasks → JSON Processing → Snowflake RAW_DB.FIELDROUTES
                      ↓
                  Watermark Updates → RAW_DB.REF.office_entity_watermark
                      ↓
              Transformation Flow → STAGING_DB.FIELDROUTES
                      ↓
              Production Views → PRODUCTION_DB.FIELDROUTES
```

### Three-Database Architecture
- **RAW_DB**: Raw JSON data from source systems
- **STAGING_DB**: Transformed, structured data ready for analytics
- **PRODUCTION_DB**: Business-facing views and reports
- Each database contains schemas for different sources (FIELDROUTES, future QUICKBOOKS, etc.)

## Development Commands

### Environment Setup
```bash
# Windows
cd "C:\Users\evanu\Documents\ETC Solutions\OctaneInsights\Prefect\altapest-prefect"
env\Scripts\activate

# WSL/Linux (if using Windows Python)
cd "/mnt/c/Users/evanu/Documents/ETC Solutions/OctaneInsights/Prefect/altapest-prefect"
./env/Scripts/python.exe
```

### Testing Commands
```bash
# Run minimal test (1 office, 1 entity, minimal API calls)
python test_direct_minimal.py

# Run comprehensive test
python test_snowflake_direct.py --entity customer --office-id 1

# Test multiple entities
python test_snowflake_direct.py --full
```

### Deployment Commands
```bash
# Deploy flows using prefect.yaml
prefect deploy --all

# Deploy specific flow
prefect deploy --name fieldroutes-nightly-snowflake-direct

# Alternative: Deploy using Python script
python -m flows.deploy_flows_snowflake
```

### Running Flows Manually
```bash
# Run nightly flow (all offices/entities)
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct'

# Run with test parameters
prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct' -p test_office_id=1 -p test_entity=customer

# Run CDC flow
prefect deployment run 'FieldRoutes_CDC_ETL_Snowflake/fieldroutes-cdc-snowflake-direct'
```

### Monitoring Commands
```sql
-- Check recent ETL runs
SELECT * FROM RAW.REF.v_etl_monitoring ORDER BY last_run_utc DESC;

-- Check specific entity loads
SELECT entity_name, office_id, last_run_utc, records_loaded, error_count
FROM RAW.REF.office_entity_watermark
WHERE office_id = 1
ORDER BY last_run_utc DESC;
```

## Code Architecture

### Core Flow Files
- `flows/fieldroutes_etl_flow_snowflake.py`: Main ETL logic with `fetch_entity()` task
- `flows/fieldroutes_cdc_flow_snowflake.py`: CDC flow for high-velocity entities
- `flows/deploy_flows_snowflake.py`: Deployment definitions and flow wrappers

### Key Components

1. **Entity Metadata (`ENTITY_META`)**: Defines all entities with their characteristics
   - Format: `(endpoint, table_name, is_dim, is_small, primary_date, secondary_date, unique_params)`
   - Located in `fieldroutes_etl_flow_snowflake.py`

2. **High-Velocity Entities (`HIGH_VELOCITY_ENTITIES`)**: Entities that change frequently and need CDC
   - Defined in `fieldroutes_cdc_flow_snowflake.py`

3. **API Integration**:
   - Rate limiting: Built-in retry logic with exponential backoff
   - Pagination: Handles large datasets with 1000-record chunks
   - Authentication: Uses Prefect Secret blocks for API credentials

4. **Snowflake Integration**:
   - Uses `prefect_snowflake.SnowflakeConnector` block
   - Bulk inserts with `executemany()`
   - JSON data stored in `RawData` column

### Error Handling Strategy

1. **Partial Success**: If one entity fails, others continue processing
2. **Retry Logic**: API calls retry 3 times with exponential backoff
3. **Watermark Safety**: 15-minute overlap in CDC to catch any missed records
4. **No Exception on Failures**: Flows complete with warnings rather than failing entirely

## Database Schema

### Main Tables (RAW.fieldroutes)
- Dimension tables: `*_Dim` (reference data like Office_Dim, Region_Dim)
- Fact tables: `*_Fact` (transactional data like Customer_Fact, Appointment_Fact)

### Table Structure
```sql
CREATE TABLE RAW.fieldroutes.<EntityName>_<Type> (
    OfficeID INT,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawData VARIANT,  -- JSON data from API
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Reference Tables (RAW.REF)
- `offices_lookup`: Office configuration and API credentials
- `office_entity_watermark`: Tracks last successful run per office/entity
- `entity_metadata`: Entity characteristics and configuration

## Important Considerations

1. **API Rate Limits**: 
   - Daily limit: 20,500 calls
   - Pauses between offices to avoid overwhelming the API
   - CDC runs during business hours only

2. **Data Volume**:
   - Some entities return 50K+ records
   - Pagination handles large datasets automatically
   - Chunk processing prevents memory issues

3. **Timezone Handling**:
   - All timestamps stored in UTC
   - Pacific timezone used for scheduling
   - `pytz` handles timezone conversions

4. **Secret Management**:
   - API credentials stored in Prefect Secret blocks
   - Block names referenced in `offices_lookup` table
   - Never hardcode credentials

5. **JSON Processing**:
   - Uses `orjson` for performance (falls back to standard `json`)
   - Snowflake VARIANT type stores raw JSON
   - Deduplication by clearing existing data for same timestamp

## Common Tasks

### Adding a New Entity
1. Add entry to `ENTITY_META` in `fieldroutes_etl_flow_snowflake.py`
2. Create corresponding table in Snowflake
3. Add to `HIGH_VELOCITY_ENTITIES` if it changes frequently
4. Test with: `python test_snowflake_direct.py --entity <new_entity> --office-id 1`

### Debugging Failed Loads
1. Check logs in Prefect UI for specific error
2. Run manual test: `python test_direct_minimal.py`
3. Check API response structure in logs
4. Verify Snowflake table exists and has correct schema

### Modifying Schedule
1. Edit `CronSchedule` in `deploy_flows_snowflake.py`
2. Redeploy: `prefect deploy --all`
3. Verify in Prefect UI

### Performance Tuning
- Adjust chunk size in `chunk_list()` function (default: 1000)
- Modify pause duration between offices
- Consider parallel processing for independent entities (but be careful of API limits)