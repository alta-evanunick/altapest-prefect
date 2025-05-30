# FieldRoutes to Snowflake Data Pipeline Architecture

## Overview

The data pipeline consists of three main layers:

1. **Extract & Load (ETL)**: Pulls data from FieldRoutes API → RAW schema
2. **Transform**: Processes RAW data → ANALYTICS schema  
3. **Reporting**: Views and materialized tables for Power BI consumption

## Data Flow

```
FieldRoutes API
    ↓ (Prefect ETL - every 30 min for CDC, nightly for full)
RAW.fieldroutes.* tables (JSON in VARIANT column)
    ↓ (Transformation flow - every hour)
ANALYTICS.* tables (Structured, typed columns)
    ↓ (SQL Views)
Reporting Views (VW_*)
    ↓
Power BI Dashboards
```

## Layer Details

### 1. RAW Layer (RAW.fieldroutes schema)
- **Purpose**: Store exact API responses without transformation
- **Structure**: 
  - `OfficeID`: Source office
  - `LoadDatetimeUTC`: When data was loaded
  - `RawData`: Complete JSON response in VARIANT column
- **Tables**: All entities from FieldRoutes API (*_DIM and *_FACT tables)
- **Refresh**: 
  - CDC mode: Every 30 minutes (last 24 hours of changes)
  - Full refresh: Nightly (all historical data)

### 2. ANALYTICS Layer (ANALYTICS schema)
- **Purpose**: Clean, structured data ready for analysis
- **Structure**: Proper columns with correct data types
- **Features**:
  - Deduplication (latest record per entity)
  - Type casting (INTEGER, STRING, FLOAT, TIMESTAMP_NTZ)
  - Incremental updates via MERGE statements
  - Primary keys enforced
- **Tables**:
  - Dimension tables: DIM_OFFICE, DIM_REGION, DIM_SERVICE_TYPE, etc.
  - Fact tables: FACT_CUSTOMER, FACT_TICKET, FACT_PAYMENT, FACT_APPLIED_PAYMENT
- **Refresh**: Every hour (incremental)

### 3. Reporting Layer (PUBLIC schema views)
- **Purpose**: Business-friendly views for Power BI
- **Views**:
  - Core entities: VW_CUSTOMER, VW_TICKET, VW_PAYMENT, etc.
  - AR Dashboard: VW_AR_AGING, VW_DSO_METRICS, VW_CEI_METRICS
  - Performance: VW_REVENUE_LEAKAGE, VW_COLLECTION_PERFORMANCE
- **Features**:
  - Renamed columns matching business terminology
  - Pre-calculated metrics
  - Optimized for Power BI DirectQuery

## Key Design Decisions

### 1. Global Unique IDs
- All IDs (CustomerID, TicketID, etc.) are globally unique across offices
- No composite keys needed
- Simplifies joins and improves performance

### 2. AppliedPayment as "Rosetta Stone"
- Links payments to invoices (tickets) for accrual-based reporting
- Enables partial payment tracking
- Critical for accurate AR calculations

### 3. Incremental Processing
- RAW layer: Captures all changes via dateUpdated fields
- ANALYTICS layer: MERGE statements prevent duplicates
- Only processes recent changes (48-hour window for safety)

### 4. Data Quality Checks
- Orphaned records detection
- Negative balance validation
- Future-dated transaction checks
- Unlinked payment verification

## Deployment

### ETL Flow
```bash
# Deploy main ETL flow
python flows/deploy_flows_snowflake.py

# Runs on schedule:
# - CDC: Every 30 minutes
# - Full: Nightly at 2 AM
```

### Transformation Flow
```bash
# Deploy transformation flow
python flows/deploy_transformation.py

# Runs every hour
# Can also run manually for full refresh
```

### Manual Execution
```python
# Run ETL for specific office
from flows.fieldroutes_etl_flow_snowflake import run_fieldroutes_etl
run_fieldroutes_etl(
    office_ids=[1],  # Seattle
    specific_entities=["customer", "ticket", "payment"],
    hours_back=24
)

# Run full transformation
from flows.transform_to_analytics_flow import transform_raw_to_analytics
transform_raw_to_analytics(incremental=False, run_quality_checks=True)
```

## Monitoring

### Check Pipeline Health
```sql
-- Recent ETL runs
SELECT 
    TableName,
    MAX(LoadDatetimeUTC) as LastLoad,
    COUNT(DISTINCT LoadDatetimeUTC) as LoadCount
FROM (
    SELECT 'CUSTOMER_FACT' as TableName, LoadDatetimeUTC 
    FROM RAW.fieldroutes.CUSTOMER_FACT
    UNION ALL
    SELECT 'TICKET_FACT', LoadDatetimeUTC 
    FROM RAW.fieldroutes.TICKET_FACT
    -- Add other tables as needed
)
GROUP BY TableName
ORDER BY LastLoad DESC;

-- Analytics table freshness
SELECT 
    'FACT_CUSTOMER' as TableName,
    COUNT(*) as RecordCount,
    MAX(LoadDatetimeUTC) as LastUpdate
FROM ANALYTICS.FACT_CUSTOMER
UNION ALL
SELECT 'FACT_TICKET', COUNT(*), MAX(LoadDatetimeUTC)
FROM ANALYTICS.FACT_TICKET;

-- Data quality summary
SELECT 
    'Orphaned Tickets' as Check,
    COUNT(*) as IssueCount
FROM ANALYTICS.FACT_TICKET t
LEFT JOIN ANALYTICS.FACT_CUSTOMER c ON t.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL;
```

## Troubleshooting

### Common Issues

1. **Missing data in analytics tables**
   - Check if RAW tables have recent data
   - Run transformation manually: `transform_raw_to_analytics(incremental=False)`

2. **Power BI refresh errors**
   - Verify views exist: `SHOW VIEWS IN PUBLIC`
   - Check permissions: User needs SELECT on all views

3. **Duplicate records**
   - Analytics layer handles deduplication automatically
   - Check LoadDatetimeUTC to see multiple versions

4. **Performance issues**
   - Consider materializing frequently-used views
   - Add indexes on join columns
   - Use table clustering for large fact tables

## Next Steps

1. **Performance Optimization**
   - Implement table clustering on date columns
   - Create materialized views for complex calculations
   - Set up query result caching

2. **Additional Metrics**
   - Customer lifetime value calculations
   - Predictive payment scoring
   - Seasonal trend analysis

3. **Data Governance**
   - Implement row-level security for multi-office access
   - Add data lineage tracking
   - Create data quality dashboards