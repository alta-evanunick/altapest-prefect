-- ========================================================================
-- Production Reporting Views for Power BI
-- These views live in PRODUCTION_DB.FIELDROUTES and reference STAGING_DB
-- ========================================================================

USE DATABASE PRODUCTION_DB;
USE SCHEMA FIELDROUTES;

-- ===== CORE ENTITY VIEWS =====

-- Customer View with exact field mappings from FR_SF_Lookup.csv
CREATE OR REPLACE VIEW VW_CUSTOMER AS
SELECT 
    CustomerID,
    OfficeID,
    FirstName,
    LastName,
    CompanyName,
    Email,
    Phone1,
    Phone2,
    Address,
    City,
    State,
    ZipCode,
    BillingCompanyName,
    BillingFirstName,
    BillingLastName,
    BillingAddress,
    BillingCity,
    BillingState,
    BillingZip,
    Balance,
    BalanceAge,
    SourceID,
    Status,
    DateAdded,
    DateUpdated,
    DateCancelled,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_CUSTOMER;

-- Employee View
CREATE OR REPLACE VIEW VW_EMPLOYEE AS
SELECT 
    e.EmployeeID,
    e.OfficeID,
    e.FirstName,
    e.LastName,
    e.Email,
    e.Phone,
    e.IsActive,
    e.IsServicePro,
    e.DateHired,
    e.DateTerminated,
    e.LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_EMPLOYEE e;

-- Appointment View  
CREATE OR REPLACE VIEW VW_APPOINTMENT AS
SELECT 
    a.AppointmentID,
    a.OfficeID,
    a.CustomerID,
    a.EmployeeID,
    a.RouteID,
    a.ServiceTypeID,
    a.ScheduledStart,
    a.ScheduledEnd,
    a.Duration,
    a.Status,
    a.DateAdded,
    a.DateUpdated,
    a.LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_APPOINTMENT a;

-- Ticket (Invoice) View
CREATE OR REPLACE VIEW VW_TICKET AS
SELECT 
    TicketID,
    OfficeID,
    CustomerID,
    SubscriptionID,
    AppointmentID,
    ServiceTypeID,
    InvoiceNumber,
    TotalAmount,
    SubtotalAmount,
    TaxAmount,
    Balance,
    ServiceCharge,
    ServiceDate,
    DateCreated,
    DateUpdated,
    CompletedOn,
    Status,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_TICKET;

-- Payment View  
CREATE OR REPLACE VIEW VW_PAYMENT AS
SELECT 
    PaymentID,
    OfficeID,
    CustomerID,
    Amount,
    AppliedAmount,
    PaymentMethod,
    CheckNumber,
    PaymentDate,
    AppliedOn,
    DateCreated,
    DateUpdated,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_PAYMENT;

-- Applied Payment View (The "Rosetta Stone")
CREATE OR REPLACE VIEW VW_APPLIED_PAYMENT AS
SELECT 
    AppliedPaymentID,
    PaymentID,
    TicketID,
    OfficeID,
    AppliedAmount,
    AppliedDate,
    DateUpdated,
    LoadDatetimeUTC,
    TRUE as IsCurrent -- For compatibility with DAX measures
FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT;

-- Route View
CREATE OR REPLACE VIEW VW_ROUTE AS
SELECT 
    r.RouteID,
    r.OfficeID,
    r.RouteName,
    r.RouteDate,
    r.EmployeeID,
    r.Status,
    r.TotalStops,
    r.CompletedStops,
    r.LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_ROUTE r;

-- Task View
CREATE OR REPLACE VIEW VW_TASK AS
SELECT 
    t.TaskID,
    t.OfficeID,
    t.CustomerID,
    t.EmployeeID,
    t.Description,
    t.DueDate,
    t.CompletedDate,
    t.Status,
    t.Priority,
    t.LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_TASK t;

-- Note View
CREATE OR REPLACE VIEW VW_NOTE AS
SELECT 
    n.NoteID,
    n.OfficeID,
    n.CustomerID,
    n.EmployeeID,
    n.NoteText,
    n.NoteType,
    n.DateAdded,
    n.LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_NOTE n;

-- ===== DIMENSION VIEWS =====

-- Office Dimension
CREATE OR REPLACE VIEW VW_OFFICE AS
SELECT 
    OfficeID,
    OfficeName,
    RegionID,
    Address,
    City,
    State,
    ZipCode,
    Phone,
    IsActive,
    LoadDatetimeUTC,
    -- Add derived fields
    CASE 
        WHEN RegionID = 1 THEN 'West'
        WHEN RegionID = 2 THEN 'East'
        WHEN RegionID = 3 THEN 'Central'
        ELSE 'Unknown'
    END as RegionName,
    State as Market
FROM STAGING_DB.FIELDROUTES.DIM_OFFICE;

-- Region Dimension
CREATE OR REPLACE VIEW VW_REGION AS
SELECT 
    RegionID,
    RegionName,
    IsActive,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_REGION;

-- Service Type Dimension
CREATE OR REPLACE VIEW VW_SERVICE_TYPE AS
SELECT 
    ServiceTypeID,
    ServiceTypeName,
    Category,
    IsRecurring,
    DefaultCharge,
    IsActive,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_SERVICE_TYPE;

-- Customer Source Dimension
CREATE OR REPLACE VIEW VW_CUSTOMER_SOURCE AS
SELECT 
    SourceID,
    SourceName,
    IsActive,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_CUSTOMER_SOURCE;

-- Product Dimension
CREATE OR REPLACE VIEW VW_PRODUCT AS
SELECT 
    ProductID,
    ProductName,
    Category,
    UnitCost,
    IsActive,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_PRODUCT;

-- Generic Flag Dimension
CREATE OR REPLACE VIEW VW_GENERIC_FLAG AS
SELECT 
    gf.GenericFlagID,
    gf.FlagName,
    gf.FlagType,
    gf.IsActive,
    gf.LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_GENERIC_FLAG gf;

-- ===== AR DASHBOARD VIEWS =====

-- AR Aging View
CREATE OR REPLACE VIEW VW_AR_AGING AS
WITH TicketAging AS (
    SELECT 
        t.CustomerID,
        t.OfficeID,
        t.TicketID,
        t.InvoiceNumber,
        t.CompletedOn,
        t.TotalAmount,
        t.Balance as BalanceAmount,
        DATEDIFF(day, t.CompletedOn, CURRENT_DATE()) as DaysOutstanding,
        CASE 
            WHEN DATEDIFF(day, t.CompletedOn, CURRENT_DATE()) <= 30 THEN '0-30 Days'
            WHEN DATEDIFF(day, t.CompletedOn, CURRENT_DATE()) <= 60 THEN '31-60 Days'
            WHEN DATEDIFF(day, t.CompletedOn, CURRENT_DATE()) <= 90 THEN '61-90 Days'
            ELSE 'Over 90 Days'
        END as AgeBucket,
        CURRENT_DATE() as AsOfDate,
        TRUE as IsCurrent
    FROM STAGING_DB.FIELDROUTES.FACT_TICKET t
    WHERE t.Balance > 0
    AND t.Status = 'Completed'
)
SELECT 
    ta.*,
    c.FirstName || ' ' || c.LastName as CustomerName,
    c.Status as CustomerStatus,
    o.OfficeName,
    o.RegionName
FROM TicketAging ta
JOIN STAGING_DB.FIELDROUTES.FACT_CUSTOMER c ON ta.CustomerID = c.CustomerID
JOIN VW_OFFICE o ON ta.OfficeID = o.OfficeID;

-- DSO Metrics View
CREATE OR REPLACE VIEW VW_DSO_METRICS AS
WITH DailyMetrics AS (
    SELECT 
        DATE_TRUNC('day', CompletedOn) as CalculationDate,
        OfficeID,
        SUM(TotalAmount) as DailyRevenue,
        SUM(Balance) as DailyARBalance
    FROM STAGING_DB.FIELDROUTES.FACT_TICKET
    WHERE Status = 'Completed'
    GROUP BY DATE_TRUNC('day', CompletedOn), OfficeID
)
SELECT 
    CalculationDate,
    OfficeID,
    DailyRevenue,
    DailyARBalance,
    AVG(DailyARBalance) OVER (
        PARTITION BY OfficeID 
        ORDER BY CalculationDate 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as AvgARBalance30Days,
    SUM(DailyRevenue) OVER (
        PARTITION BY OfficeID 
        ORDER BY CalculationDate 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as TotalRevenue30Days
FROM DailyMetrics;

-- CEI Metrics View
CREATE OR REPLACE VIEW VW_CEI_METRICS AS
WITH MonthlyMetrics AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) as CalculationDate,
        t.OfficeID,
        SUM(CASE WHEN t.CompletedOn >= DATE_TRUNC('month', CURRENT_DATE()) 
                 THEN t.TotalAmount ELSE 0 END) as MonthlyInvoiced,
        SUM(CASE WHEN p.AppliedOn >= DATE_TRUNC('month', CURRENT_DATE())
                 THEN ap.AppliedAmount ELSE 0 END) as MonthlyCollected
    FROM STAGING_DB.FIELDROUTES.FACT_TICKET t
    LEFT JOIN STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT ap ON t.TicketID = ap.TicketID
    LEFT JOIN STAGING_DB.FIELDROUTES.FACT_PAYMENT p ON ap.PaymentID = p.PaymentID
    WHERE t.Status = 'Completed'
    GROUP BY t.OfficeID
)
SELECT 
    CalculationDate,
    OfficeID,
    MonthlyInvoiced,
    MonthlyCollected,
    CASE 
        WHEN MonthlyInvoiced > 0 
        THEN (MonthlyCollected / MonthlyInvoiced) * 100
        ELSE 0
    END as CollectionEfficiencyIndex
FROM MonthlyMetrics;

-- AR Waterfall View
CREATE OR REPLACE VIEW VW_AR_WATERFALL AS
SELECT 
    'Beginning AR' as Category,
    1 as SortOrder,
    SUM(CASE WHEN CompletedOn < DATE_TRUNC('month', CURRENT_DATE()) 
             THEN Balance ELSE 0 END) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_TICKET
WHERE Status = 'Completed'

UNION ALL

SELECT 
    'New Invoices' as Category,
    2 as SortOrder,
    SUM(CASE WHEN CompletedOn >= DATE_TRUNC('month', CURRENT_DATE())
             THEN TotalAmount ELSE 0 END) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_TICKET
WHERE Status = 'Completed'

UNION ALL

SELECT 
    'Collections' as Category,
    3 as SortOrder,
    -1 * SUM(ap.AppliedAmount) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT ap
JOIN STAGING_DB.FIELDROUTES.FACT_PAYMENT p ON ap.PaymentID = p.PaymentID
WHERE p.AppliedOn >= DATE_TRUNC('month', CURRENT_DATE())

UNION ALL

SELECT 
    'Write-offs' as Category,
    4 as SortOrder,
    -1 * SUM(Amount) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_CREDIT_MEMO
WHERE Type = 'Write-off'
AND CreatedDate >= DATE_TRUNC('month', CURRENT_DATE())

UNION ALL

SELECT 
    'Ending AR' as Category,
    5 as SortOrder,
    SUM(Balance) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_TICKET
WHERE Status = 'Completed';

-- Payment Velocity View
CREATE OR REPLACE VIEW VW_PAYMENT_VELOCITY AS
SELECT 
    ap.AppliedPaymentID,
    ap.TicketID,
    ap.PaymentID,
    ap.OfficeID,
    t.CustomerID,
    t.CompletedOn as InvoiceDate,
    p.AppliedOn as PaymentDate,
    DATEDIFF(day, t.CompletedOn, p.AppliedOn) as DaysToPay,
    ap.AppliedAmount,
    t.TotalAmount as InvoiceAmount
FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT ap
JOIN STAGING_DB.FIELDROUTES.FACT_TICKET t ON ap.TicketID = t.TicketID
JOIN STAGING_DB.FIELDROUTES.FACT_PAYMENT p ON ap.PaymentID = p.PaymentID
WHERE t.Status = 'Completed'
AND p.AppliedOn IS NOT NULL;

-- Credit Memo View (placeholder - create table if doesn't exist)
CREATE OR REPLACE VIEW VW_CREDIT_MEMO AS
SELECT 
    NULL::INTEGER as CreditMemoID,
    NULL::INTEGER as TicketID,
    NULL::INTEGER as CustomerID,
    NULL::FLOAT as Amount,
    NULL::VARCHAR as Type,
    NULL::TIMESTAMP_NTZ as CreatedDate
WHERE 1=0;  -- Empty view until table is created

-- ===== PERFORMANCE DASHBOARD VIEWS =====

-- Revenue Leakage View
CREATE OR REPLACE VIEW VW_REVENUE_LEAKAGE AS
WITH ServiceMetrics AS (
    SELECT 
        DATE_TRUNC('month', ServiceDate) as ServiceMonth,
        OfficeID,
        COUNT(DISTINCT TicketID) as TotalServices,
        COUNT(DISTINCT CASE WHEN Status = 'Completed' THEN TicketID END) as CompletedServices,
        COUNT(DISTINCT CASE WHEN Status = 'Cancelled' THEN TicketID END) as CancelledServices,
        SUM(CASE WHEN Status = 'Completed' THEN TotalAmount ELSE 0 END) as CompletedRevenue,
        SUM(CASE WHEN Status = 'Cancelled' THEN TotalAmount ELSE 0 END) as LostRevenue
    FROM STAGING_DB.FIELDROUTES.FACT_TICKET
    GROUP BY DATE_TRUNC('month', ServiceDate), OfficeID
)
SELECT 
    ServiceMonth,
    OfficeID,
    TotalServices,
    CompletedServices,
    CancelledServices,
    CompletedRevenue,
    LostRevenue,
    CASE 
        WHEN TotalServices > 0 
        THEN (CancelledServices::FLOAT / TotalServices) * 100
        ELSE 0 
    END as CancellationRate
FROM ServiceMetrics;

-- Collection Performance View
CREATE OR REPLACE VIEW VW_COLLECTION_PERFORMANCE AS
SELECT 
    DATE_TRUNC('month', p.AppliedOn) as CollectionMonth,
    p.OfficeID,
    COUNT(DISTINCT p.CustomerID) as PayingCustomers,
    COUNT(DISTINCT p.PaymentID) as TotalPayments,
    SUM(p.Amount) as TotalCollected,
    AVG(p.Amount) as AvgPaymentAmount,
    COUNT(DISTINCT CASE WHEN p.PaymentMethod = 'Credit Card' THEN p.PaymentID END) as CreditCardPayments,
    COUNT(DISTINCT CASE WHEN p.PaymentMethod = 'Check' THEN p.PaymentID END) as CheckPayments,
    COUNT(DISTINCT CASE WHEN p.PaymentMethod = 'Cash' THEN p.PaymentID END) as CashPayments,
    COUNT(DISTINCT CASE WHEN p.PaymentMethod = 'ACH' THEN p.PaymentID END) as ACHPayments
FROM STAGING_DB.FIELDROUTES.FACT_PAYMENT p
WHERE p.AppliedOn IS NOT NULL
GROUP BY DATE_TRUNC('month', p.AppliedOn), p.OfficeID;

-- ===== GRANT PERMISSIONS =====
-- Grant SELECT on all views to appropriate roles
GRANT SELECT ON ALL VIEWS IN SCHEMA PRODUCTION_DB.FIELDROUTES TO ROLE ACCOUNTADMIN;

-- Create a read-only role for Power BI if needed
CREATE ROLE IF NOT EXISTS POWERBI_READER;
GRANT USAGE ON DATABASE PRODUCTION_DB TO ROLE POWERBI_READER;
GRANT USAGE ON SCHEMA PRODUCTION_DB.FIELDROUTES TO ROLE POWERBI_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA PRODUCTION_DB.FIELDROUTES TO ROLE POWERBI_READER;