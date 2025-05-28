# Alta Pest Control Analytics Project Context

## Current Status (as of 2025-05-27)

### ✅ What We've Accomplished Today
1. **Implemented Direct FieldRoutes → Snowflake Pipeline**
   - Removed Azure Blob Storage intermediary
   - Created new flow files (`*_snowflake.py` versions)
   - Tested successfully with office 1 customer entity
   - Merged PR to main branch
   - Ready for full deployment after API limit reset

2. **Key Technical Details**
   - API Limit: 20.5K calls per day per office
   - Test used only 2 API calls (minimal impact)
   - Data lands in `RAW.fieldroutes.*` tables in Snowflake
   - Each record contains: OfficeID, LoadDatetimeUTC, RawData (JSON)

### 🎯 Ultimate Goal
Build a robust PowerBI reporting and analytics suite for Alta Pest Control using the FieldRoutes data now flowing into Snowflake.

## Roadmap: From Raw Data to PowerBI Analytics

### Phase 1: Data Pipeline Stabilization (Tomorrow - Day 1)
- [ ] Deploy new flows after API limit reset
  ```bash
  python -m flows.deploy_flows_snowflake
  ```
- [ ] Run initial nightly load for all offices
- [ ] Monitor API usage and performance
- [ ] Verify all entity types load correctly
- [ ] Set up error alerting in Prefect

### Phase 2: Data Transformation Layer (Days 2-3)
- [ ] Create staging views to parse JSON data
- [ ] Build dimension tables (Customer, Employee, Office, ServiceType)
- [ ] Create fact tables (Appointments, Tickets, Payments, Routes)
- [ ] Implement slowly changing dimension (SCD) logic for tracking changes
- [ ] Add data quality checks and validation

### Phase 3: Analytics Data Model (Days 4-5)
- [ ] Design star schema for PowerBI consumption
- [ ] Create aggregated tables for common metrics:
  - Daily/Weekly/Monthly service counts
  - Revenue by office/technician/service type
  - Customer acquisition and retention metrics
  - Route efficiency metrics
- [ ] Build incremental refresh logic
- [ ] Create documentation for data model

### Phase 4: PowerBI Development (Days 6-8)
- [ ] Set up PowerBI Gateway for Snowflake connection
- [ ] Import data model into PowerBI
- [ ] Create base measures and calculated columns
- [ ] Build initial dashboards based on Alta_Dash_Plan.docx:
  - Executive Summary Dashboard
  - Operations Dashboard
  - Financial Performance Dashboard
  - Technician Performance Dashboard
- [ ] Implement row-level security for office access

### Phase 5: Testing & Optimization (Days 9-10)
- [ ] User acceptance testing with Alta team
- [ ] Performance optimization (aggregations, indexing)
- [ ] Set up automated refresh schedules
- [ ] Create user documentation
- [ ] Train end users

## Key Files and Locations

### Data Pipeline Files
- **ETL Flow**: `/flows/fieldroutes_etl_flow_snowflake.py`
- **CDC Flow**: `/flows/fieldroutes_cdc_flow_snowflake.py`
- **Deployment**: `/flows/deploy_flows_snowflake.py`
- **Test Script**: `test_snowflake_direct.py`

### Documentation
- **Migration Guide**: `MIGRATION_GUIDE.md`
- **Test Instructions**: `TEST_INSTRUCTIONS.md`
- **This Context**: `PROJECT_CONTEXT.md`

### Important Queries
```sql
-- Check data load status
SELECT * FROM RAW.REF.v_etl_monitoring;

-- View raw customer data
SELECT * FROM RAW.fieldroutes.Customer_Dim 
WHERE OfficeID = 1 
ORDER BY _loaded_at DESC LIMIT 10;

-- Count records by entity
SELECT 
    table_name,
    row_count
FROM INFORMATION_SCHEMA.TABLES t
JOIN (
    SELECT 'Customer_Dim' as table_name, COUNT(*) as row_count FROM RAW.fieldroutes.Customer_Dim
    UNION ALL
    SELECT 'Appointment_Fact', COUNT(*) FROM RAW.fieldroutes.Appointment_Fact
    -- Add other tables as needed
) counts USING (table_name);
```

## Tomorrow's First Steps

1. **Check API limit reset** (usually midnight)
2. **Deploy the flows**:
   ```bash
   cd "C:\Users\evanu\Documents\ETC Solutions\OctaneInsights\Prefect\altapest-prefect"
   env\Scripts\activate
   python -m flows.deploy_flows_snowflake
   ```

3. **Run initial test load**:
   ```bash
   prefect deployment run 'FieldRoutes_Nightly_ETL_Snowflake/fieldroutes-nightly-snowflake-direct' -p test_office_id=1
   ```

4. **Monitor in Snowflake**:
   ```sql
   SELECT * FROM RAW.REF.v_etl_monitoring ORDER BY last_run_utc DESC;
   ```

5. **Begin Phase 2** - Start creating staging views

## Context for Claude Code

When you return tomorrow, mention:
- "Continue Alta Pest Control analytics project"
- "Deployed direct Snowflake pipeline, ready for Phase 2"
- "Need to build transformation layer for PowerBI"
- Reference this file: `PROJECT_CONTEXT.md`

## Technical Stack
- **Source**: FieldRoutes API
- **Orchestration**: Prefect
- **Data Warehouse**: Snowflake (RAW → STAGING → ANALYTICS schemas)
- **Visualization**: PowerBI
- **Client**: Alta Pest Control

## Contact/Resources
- Dashboard Plan: `C:\Users\evanu\Documents\ETC Solutions\OctaneInsights\Alta\Alta_Dash_Plan.docx`
- GitHub Repo: https://github.com/alta-evanunick/altapest-prefect
- Prefect Blocks: "snowflake-altapestdb"

---
Last Updated: 2025-05-27 23:30 UTC
Next Session: Deploy flows after API reset, begin transformation layer