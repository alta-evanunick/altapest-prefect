# RAW → ANALYTICS Implementation Checklist

## Pre-Implementation Requirements ✅
- [x] Snowflake connection established
- [x] RAW schema with data populated
- [x] Prefect environment configured
- [x] Power BI Desktop connected to Snowflake

## Phase 1: Deploy New Flows (Required)

### 1. Update Prefect Deployments
```bash
# Remove old deployments first (if any exist)
prefect deployment delete "FieldRoutes_Nightly_ETL/fieldroutes-nightly-snowflake-direct" --yes
prefect deployment delete "FieldRoutes_CDC_ETL/fieldroutes-cdc-snowflake-direct" --yes

# Deploy updated flows (3 deployments total)
cd /mnt/c/Users/evanu/Documents/ETC\ Solutions/OctaneInsights/Prefect/altapest-prefect
python flows/deploy_flows_snowflake.py
```

**Expected Output:**
```
✅ [1/3] Created: fieldroutes-nightly-etl
✅ [2/3] Created: fieldroutes-cdc-etl  
✅ [3/3] Created: raw-to-analytics-transform
Using 3 of your 5 available deployment slots
```

### 2. Create ANALYTICS Schema
```sql
-- Run in Snowflake
USE DATABASE ALTAPEST_DB;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE ACCOUNTADMIN;
GRANT ALL ON SCHEMA ANALYTICS TO ROLE ACCOUNTADMIN;
```

## Phase 2: First-Time Setup (Run Once)

### 3. Initial Full Transformation
```bash
# Run manual full transformation to populate ANALYTICS tables
prefect deployment run 'transform-raw-to-analytics/raw-to-analytics-transform' \
  --param incremental=false \
  --param run_quality_checks=true
```

**Verify Success:**
```sql
-- Check if ANALYTICS tables were created
SHOW TABLES IN ANALYTICS;

-- Verify data counts
SELECT 'DIM_OFFICE' as TableName, COUNT(*) as RecordCount FROM ANALYTICS.DIM_OFFICE
UNION ALL
SELECT 'FACT_CUSTOMER', COUNT(*) FROM ANALYTICS.FACT_CUSTOMER  
UNION ALL
SELECT 'FACT_TICKET', COUNT(*) FROM ANALYTICS.FACT_TICKET
UNION ALL
SELECT 'FACT_PAYMENT', COUNT(*) FROM ANALYTICS.FACT_PAYMENT
UNION ALL
SELECT 'FACT_APPLIED_PAYMENT', COUNT(*) FROM ANALYTICS.FACT_APPLIED_PAYMENT;
```

### 4. Update Reporting Views (If Not Already Done)
```sql
-- Run the updated reporting views script
-- This should point to ANALYTICS schema instead of RAW
-- File: /sql/create_reporting_views_v3.sql (needs to be created)
```

**Create Updated Views Script:**
```sql
-- Example update needed in views:
-- OLD: FROM RAW.fieldroutes.CUSTOMER_FACT
-- NEW: FROM ANALYTICS.FACT_CUSTOMER
```

## Phase 3: Validation & Testing

### 5. Data Quality Validation
```sql
-- Check for data quality issues
SELECT 
    'Orphaned Tickets' as Issue,
    COUNT(*) as Count
FROM ANALYTICS.FACT_TICKET t
LEFT JOIN ANALYTICS.FACT_CUSTOMER c ON t.CustomerID = c.CustomerID
WHERE c.CustomerID IS NULL

UNION ALL

SELECT 
    'Unlinked Applied Payments',
    COUNT(*)
FROM ANALYTICS.FACT_APPLIED_PAYMENT ap
LEFT JOIN ANALYTICS.FACT_TICKET t ON ap.TicketID = t.TicketID
WHERE t.TicketID IS NULL

UNION ALL

SELECT 
    'Negative Customer Balances',
    COUNT(*)
FROM ANALYTICS.FACT_CUSTOMER
WHERE Balance < 0

UNION ALL

SELECT 
    'Future Dated Tickets',
    COUNT(*)
FROM ANALYTICS.FACT_TICKET
WHERE CompletedOn > CURRENT_TIMESTAMP();
```

### 6. Test Power BI Connection
```sql
-- Test key views that Power BI will use
SELECT TOP 100 * FROM VW_AR_AGING;
SELECT TOP 100 * FROM VW_CUSTOMER;
SELECT TOP 100 * FROM VW_TICKET;
SELECT TOP 100 * FROM VW_PAYMENT;
SELECT TOP 100 * FROM VW_APPLIED_PAYMENT;
```

### 7. Performance Testing
```sql
-- Test query performance for Power BI
SELECT 
    COUNT(*) as TotalTickets,
    SUM(TotalAmount) as TotalRevenue,
    SUM(Balance) as OutstandingAR
FROM VW_TICKET
WHERE CompletedOn >= DATEADD(month, -12, CURRENT_DATE());

-- Test AR aging performance
SELECT 
    AgeBucket,
    COUNT(*) as CustomerCount,
    SUM(BalanceAmount) as TotalBalance
FROM VW_AR_AGING
WHERE IsCurrent = TRUE
GROUP BY AgeBucket;
```

## Phase 4: Monitoring Setup

### 8. Schedule Monitoring Queries
```sql
-- Create monitoring view
CREATE OR REPLACE VIEW ANALYTICS.VW_PIPELINE_STATUS AS
SELECT 
    'RAW_CUSTOMER' as TableName,
    COUNT(*) as RecordCount,
    MAX(LoadDatetimeUTC) as LastUpdate,
    DATEDIFF(hour, MAX(LoadDatetimeUTC), CURRENT_TIMESTAMP()) as HoursSinceUpdate
FROM RAW.fieldroutes.CUSTOMER_FACT
UNION ALL
SELECT 
    'ANALYTICS_CUSTOMER',
    COUNT(*),
    MAX(LoadDatetimeUTC),
    DATEDIFF(hour, MAX(LoadDatetimeUTC), CURRENT_TIMESTAMP())
FROM ANALYTICS.FACT_CUSTOMER;
```

### 9. Set Up Alerts (Optional)
```python
# Add to transform_to_analytics_flow.py if needed
@task
def check_pipeline_health(snowflake: SnowflakeConnector):
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TableName, HoursSinceUpdate 
            FROM ANALYTICS.VW_PIPELINE_STATUS 
            WHERE HoursSinceUpdate > 25
        """)
        stale_tables = cursor.fetchall()
        
        if stale_tables:
            logger.warning(f"Stale tables detected: {stale_tables}")
            # Could send email/Slack alert here
```

## Phase 5: Power BI Integration

### 10. Update Power BI Data Sources
1. **Open Power BI Desktop**
2. **Transform Data → Data source settings**
3. **Update connection** to use new views if needed
4. **Refresh data** to test new schema

### 11. Deploy DAX Measures
1. **Copy measures** from `/dax/AR_Dashboard_Measures.dax`
2. **Create new measures** in Power BI
3. **Test calculations** against known data
4. **Create initial dashboard** using new measures

### 12. Performance Optimization
```sql
-- Add clustering if performance is slow
ALTER TABLE ANALYTICS.FACT_TICKET 
CLUSTER BY (CompletedOn, CustomerID);

ALTER TABLE ANALYTICS.FACT_CUSTOMER 
CLUSTER BY (OfficeID, CustomerID);

-- Consider materialized views for complex calculations
CREATE OR REPLACE VIEW ANALYTICS.VW_AR_SUMMARY_MATERIALIZED AS
SELECT 
    CustomerID,
    SUM(Balance) as TotalBalance,
    DATEDIFF(day, MIN(CompletedOn), CURRENT_DATE()) as OldestInvoiceAge
FROM ANALYTICS.FACT_TICKET
WHERE Balance > 0
GROUP BY CustomerID;
```

## Troubleshooting Guide

### Common Issues & Solutions

#### 1. "Table doesn't exist" errors
```sql
-- Check if ANALYTICS schema exists
SHOW SCHEMAS IN DATABASE ALTAPEST_DB;

-- Re-run transformation
prefect deployment run 'transform-raw-to-analytics/raw-to-analytics-transform' --param incremental=false
```

#### 2. Zero records in ANALYTICS tables
```sql
-- Check RAW data exists
SELECT COUNT(*) FROM RAW.fieldroutes.CUSTOMER_FACT;

-- Check LoadDatetimeUTC distribution
SELECT 
    DATE(LoadDatetimeUTC) as LoadDate,
    COUNT(*) as RecordCount
FROM RAW.fieldroutes.CUSTOMER_FACT
GROUP BY DATE(LoadDatetimeUTC)
ORDER BY LoadDate DESC;
```

#### 3. Power BI connection issues
- Verify user has SELECT permissions on all ANALYTICS tables
- Check that views reference ANALYTICS schema correctly
- Test connection with simple query first

#### 4. Performance issues
- Check warehouse size (may need to scale up temporarily)
- Verify clustering is working: `SHOW TABLES LIKE 'FACT_%' IN ANALYTICS;`
- Consider materializing frequently-used views

## Success Criteria

✅ **Phase 1 Complete When:**
- All 3 Prefect deployments created successfully
- No old deployments consuming slots

✅ **Phase 2 Complete When:**
- ANALYTICS schema exists with all tables
- Initial transformation runs successfully
- Data quality checks pass

✅ **Phase 3 Complete When:**
- All views return data
- Power BI can connect and refresh
- Performance is acceptable

✅ **Production Ready When:**
- Scheduled flows running automatically
- Monitoring in place
- Power BI dashboards operational

## Next Steps After Implementation

1. **Monitor for 1 week** to ensure stability
2. **Optimize performance** based on usage patterns  
3. **Add additional metrics** as business needs evolve
4. **Consider upgrading Snowflake** for real-time processing (Streams/Tasks)