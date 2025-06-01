-- ========================================================================
-- Production Reporting Views for Power BI
-- These views live in PRODUCTION_DB.FIELDROUTES and reference STAGING_DB
-- Updated to match latest table structures and naming conventions
-- ========================================================================

USE DATABASE PRODUCTION_DB;
CREATE SCHEMA IF NOT EXISTS FIELDROUTES;
USE SCHEMA FIELDROUTES;

-- ===== CORE ENTITY VIEWS =====

-- Customer View with exact field mappings from table structure
CREATE OR REPLACE VIEW VW_CUSTOMER AS
SELECT 
    CustomerID,
    BillToAccountID,
    OfficeID,
    FName as FirstName,
    LName as LastName,
    CompanyName,
    Spouse,
    IsCommercial,
    Status,
    StatusText,
    Email,
    Phone1,
    Phone1_Ext,
    Phone2,
    Phone2_Ext,
    Address,
    City,
    State,
    Zip as ZipCode,
    BillingCompanyName,
    BillingFName as BillingFirstName,
    BillingLName as BillingLastName,
    BillingAddress,
    BillingCity,
    BillingState,
    BillingZip,
    BillingPhone,
    BillingEmail,
    Latitude,
    Longitude,
    SqFeet,
    AddedByEmployeeID,
    DateAdded,
    DateCancelled,
    DateUpdated,
    SourceID,
    Source,
    APay,
    PreferredTechID,
    PaidInFull,
    SubscriptionIDs,
    Balance,
    BalanceAge,
    ResponsibleBalance,
    ResponsibleBalanceAge,
    MasterAccount,
    PreferredBillingDate,
    PaymentHoldDate,
    LatestCCLastFour,
    LatestCCExpDate,
    AppointmentIDs,
    TicketIDs,
    PaymentIDs,
    RegionID,
    SpecialScheduling,
    TaxRate,
    StateTax,
    CityTax,
    CountyTax,
    DistrictTax,
    CustomTax,
    ZipTaxID,
    SMSReminders,
    PhoneReminders,
    EmailReminders,
    CustomerSource,
    CustomerSourceID,
    MaxMonthlyCharge,
    County,
    AutopayPaymentProfileID,
    DivisionID,
    AgingDate,
    ResponsibleAgingDate,
    SalesmanAPay,
    TermiteMonitoring,
    PendingCancel,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_CUSTOMER;

-- Employee View
CREATE OR REPLACE VIEW VW_EMPLOYEE AS
SELECT 
    EmployeeID,
    OfficeID,
    IsActive,
    FName as FirstName,
    LName as LastName,
    EmployeeTypeText,
    Address,
    City,
    State,
    Zip,
    Phone,
    Email,
    Experience,
    SkillIDs,
    SkillDescriptions,
    LinkedEmployeeIDs,
    EmployeeLink,
    LicenseNumber,
    SupervisorID,
    RoamingRep,
    RegionalManagerOfficeIDs,
    LastLogin,
    TeamIDs,
    PrimaryTeam,
    AccessControlProfileID,
    StartAddress,
    StartCity,
    StartState,
    StartZip,
    StartLatitude,
    StartLongitude,
    DateHired,
    DateUpdated,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_EMPLOYEE;

-- Appointment View  
CREATE OR REPLACE VIEW VW_APPOINTMENT AS
SELECT 
    AppointmentID,
    OfficeID,
    CustomerID,
    SubscriptionID,
    SubscriptionRegionID,
    RouteID,
    SpotID,
    AppointmentDate,
    StartTime,
    EndTime,
    TimeWindow,
    Duration,
    AppointmentType,
    DateAdded,
    EmployeeID,
    Status,
    StatusText,
    CallAhead,
    IsInitial,
    SubscriptionPreferredTech,
    CompletedBy,
    ServicedBy,
    DateCompleted,
    AppointmentNotes,
    OfficeNotes,
    TimeIn,
    TimeOut,
    CheckIn,
    CheckOut,
    WindSpeed,
    WindDirection,
    Temperature,
    IsInterior,
    TicketID,
    DateCancelled,
    AdditionalTechs,
    NotificationPreference,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_APPOINTMENT;

-- Ticket (Invoice) View
CREATE OR REPLACE VIEW VW_TICKET AS
SELECT 
    TicketID,
    AppointmentID,
    CustomerID,
    BillToAccountID,
    OfficeID,
    DateCreated,
    InvoiceDate,
    DateUpdated,
    IsActive,
    Subtotal,
    TaxAmount,
    Total as TotalAmount,
    ServiceCharge,
    ServiceTaxable,
    ProductionValue,
    TaxRate,
    Balance,
    SubscriptionID,
    ServiceID,
    Items,
    CreatedByEmployeeID,
    CompletedOn,
    ServiceTypeID,
    Status,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_TICKET;

-- Payment View  
CREATE OR REPLACE VIEW VW_PAYMENT AS
SELECT 
    PaymentID,
    OfficeID,
    CustomerID,
    PaymentDate,
    PaymentMethod,
    Amount,
    AppliedAmount,
    UnassignedAmount,
    Status,
    InvoiceIDs,
    PaymentApplications,
    EmployeeIDs,
    IsOfficePayment,
    IsCollectionPayment,
    Writeoff,
    CreditMemo,
    PaymentOrigin,
    OriginalPaymentID,
    LastFour,
    PaymentNotes,
    BatchOpened,
    BatchClosed,
    PaymentSource,
    DateUpdated,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_PAYMENT;

-- Applied Payment View (The "Rosetta Stone")
CREATE OR REPLACE VIEW VW_APPLIED_PAYMENT AS
SELECT 
    AppliedPaymentID,
    OfficeID,
    PaymentID,
    TicketID,
    CustomerID,
    DateApplied,
    AppliedAmount,
    DateUpdated,
    LoadDatetimeUTC,
    TRUE as IsCurrent -- For compatibility with DAX measures
FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT;

-- Route View
CREATE OR REPLACE VIEW VW_ROUTE AS
SELECT 
    RouteID,
    OfficeID,
    DateCreated,
    DateUpdated,
    RouteDate,
    RouteEmployeeID,
    SpotsSold,
    SpotsOpen,
    Status,
    DateCompleted,
    DateCancelled,
    StartTime,
    EndTime,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_ROUTE;

-- Task View
CREATE OR REPLACE VIEW VW_TASK AS
SELECT 
    TaskID,
    OfficeID,
    CustomerID,
    SubscriptionID,
    TicketID,
    AppointmentID,
    TaskType,
    TaskDescription,
    Status,
    NeedsApproval,
    ApprovalStatus,
    AssignedTo,
    CreatedBy,
    DateCreated,
    DueDate,
    DateCompleted,
    DateUpdated,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_TASK;

-- Note View
CREATE OR REPLACE VIEW VW_NOTE AS
SELECT 
    NoteID,
    OfficeID,
    CustomerID,
    EmployeeID,
    EmployeeName,
    DateCreated,
    IsVisibleCustomer,
    IsVisibleTechnician,
    CancellationReasonID,
    CancellationReason,
    TypeID,
    TypeDescription,
    NoteContent,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.FACT_NOTE;

-- ===== DIMENSION VIEWS =====

-- Office Dimension
CREATE OR REPLACE VIEW VW_OFFICE AS
SELECT 
    OfficeID,
    OfficeName,
    CompanyID,
    LicenseNumber,
    ContactNumber,
    ContactEmail,
    Timezone,
    Address,
    City,
    State,
    ZipCode,
    CautionStatements,
    OfficeLatitude,
    OfficeLongitude,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_OFFICE;

-- Region Dimension
CREATE OR REPLACE VIEW VW_REGION AS
SELECT 
    RegionID,
    OfficeID,
    DateCreated,
    DateDeleted,
    LatAndLong,
    RegionType,
    IsActive,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_REGION;

-- Service Type Dimension
CREATE OR REPLACE VIEW VW_SERVICE_TYPE AS
SELECT 
    ServiceTypeID,
    OfficeID,
    ServiceName,
    Description,
    Frequency,
    DefaultCharge,
    Category,
    IsReserviceType,
    DefaultAppointmentLength,
    DefaultInitialCharge,
    InitialID,
    MinRecurringCharge,
    MinInitialCharge,
    IsRegularService,
    IsInitialService,
    SeasonStart,
    SeasonEnd,
    SentriconServiceType,
    IsVisible,
    DefaultFollowupDelay,
    SalesVisible,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_SERVICETYPE;

-- Customer Source Dimension
CREATE OR REPLACE VIEW VW_CUSTOMER_SOURCE AS
SELECT 
    SourceID,
    OfficeID,
    SourceName,
    IsSalesroutesDefault,
    IsVisible,
    IsDealsSource,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_CUSTOMERSOURCE;

-- Product Dimension
CREATE OR REPLACE VIEW VW_PRODUCT AS
SELECT 
    ProductID,
    OfficeID,
    Description,
    GLAccountID,
    Amount,
    Taxable,
    ProductCode,
    ProductCategory,
    IsVisible,
    IsSalesVisible,
    IsRecurring,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_PRODUCT;

-- Generic Flag Dimension
CREATE OR REPLACE VIEW VW_GENERIC_FLAG AS
SELECT 
    GenericFlagID,
    OfficeID,
    FlagCode,
    FlagDescription,
    FlagStatus,
    FlagType,
    DateCreated,
    DateUpdated,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_GENERICFLAG;

-- Cancellation Reason Dimension
CREATE OR REPLACE VIEW VW_CANCELLATION_REASON AS
SELECT 
    CancellationReasonID,
    OfficeID,
    ReasonName,
    IsActive,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_CANCELLATIONREASON;

-- Re-Service Reason Dimension
CREATE OR REPLACE VIEW VW_RESERVICEREASON AS
SELECT 
    ReserviceReasonID,
    OfficeID,
    IsVisible,
    Description,
    LoadDatetimeUTC
FROM STAGING_DB.FIELDROUTES.DIM_RESERVICEREASON;

-- ===== AR DASHBOARD VIEWS =====

-- AR Aging View
CREATE OR REPLACE VIEW VW_AR_AGING AS
WITH TicketAging AS (
    SELECT 
        t.CustomerID,
        t.OfficeID,
        t.TicketID,
        t.CompletedOn,
        t.Total as TotalAmount,
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
    c.FName || ' ' || c.LName as CustomerName,
    c.CompanyName,
    c.Status as CustomerStatus,
    o.OfficeName
FROM TicketAging ta
JOIN STAGING_DB.FIELDROUTES.FACT_CUSTOMER c ON ta.CustomerID = c.CustomerID
JOIN VW_OFFICE o ON ta.OfficeID = o.OfficeID;

-- DSO Metrics View
CREATE OR REPLACE VIEW VW_DSO_METRICS AS
WITH DailyMetrics AS (
    SELECT 
        DATE_TRUNC('day', CompletedOn) as CalculationDate,
        OfficeID,
        SUM(Total) as DailyRevenue,
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
    ) as TotalRevenue30Days,
    -- Calculate DSO
    CASE 
        WHEN SUM(DailyRevenue) OVER (
            PARTITION BY OfficeID 
            ORDER BY CalculationDate 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) > 0 
        THEN (AVG(DailyARBalance) OVER (
            PARTITION BY OfficeID 
            ORDER BY CalculationDate 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) / SUM(DailyRevenue) OVER (
            PARTITION BY OfficeID 
            ORDER BY CalculationDate 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )) * 30
        ELSE 0
    END as DSO
FROM DailyMetrics;

-- CEI Metrics View
CREATE OR REPLACE VIEW VW_CEI_METRICS AS
WITH MonthlyMetrics AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) as CalculationDate,
        t.OfficeID,
        SUM(CASE WHEN t.CompletedOn >= DATE_TRUNC('month', CURRENT_DATE()) 
                 THEN t.Total ELSE 0 END) as MonthlyInvoiced,
        SUM(CASE WHEN p.PaymentDate >= DATE_TRUNC('month', CURRENT_DATE())
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
             THEN Total ELSE 0 END) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_TICKET
WHERE Status = 'Completed'

UNION ALL

SELECT 
    'Collections' as Category,
    3 as SortOrder,
    -1 * SUM(ap.AppliedAmount) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT ap
JOIN STAGING_DB.FIELDROUTES.FACT_PAYMENT p ON ap.PaymentID = p.PaymentID
WHERE p.PaymentDate >= DATE_TRUNC('month', CURRENT_DATE())

UNION ALL

SELECT 
    'Write-offs' as Category,
    4 as SortOrder,
    -1 * SUM(CASE WHEN Writeoff = 1 THEN Amount ELSE 0 END) as Amount
FROM STAGING_DB.FIELDROUTES.FACT_PAYMENT
WHERE PaymentDate >= DATE_TRUNC('month', CURRENT_DATE())

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
    p.PaymentDate,
    DATEDIFF(day, t.CompletedOn, p.PaymentDate) as DaysToPay,
    ap.AppliedAmount,
    t.Total as InvoiceAmount
FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT ap
JOIN STAGING_DB.FIELDROUTES.FACT_TICKET t ON ap.TicketID = t.TicketID
JOIN STAGING_DB.FIELDROUTES.FACT_PAYMENT p ON ap.PaymentID = p.PaymentID
WHERE t.Status = 'Completed'
AND p.PaymentDate IS NOT NULL;

-- ===== PERFORMANCE DASHBOARD VIEWS =====

-- Revenue Leakage View
CREATE OR REPLACE VIEW VW_REVENUE_LEAKAGE AS
WITH ServiceMetrics AS (
    SELECT 
        DATE_TRUNC('month', CompletedOn) as ServiceMonth,
        OfficeID,
        COUNT(DISTINCT TicketID) as TotalServices,
        COUNT(DISTINCT CASE WHEN Status = 'Completed' THEN TicketID END) as CompletedServices,
        COUNT(DISTINCT CASE WHEN Status = 'Cancelled' THEN TicketID END) as CancelledServices,
        SUM(CASE WHEN Status = 'Completed' THEN Total ELSE 0 END) as CompletedRevenue,
        SUM(CASE WHEN Status = 'Cancelled' THEN Total ELSE 0 END) as LostRevenue
    FROM STAGING_DB.FIELDROUTES.FACT_TICKET
    GROUP BY DATE_TRUNC('month', CompletedOn), OfficeID
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
    DATE_TRUNC('month', p.PaymentDate) as CollectionMonth,
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
WHERE p.PaymentDate IS NOT NULL
GROUP BY DATE_TRUNC('month', p.PaymentDate), p.OfficeID;

-- Customer Lifetime Value View
CREATE OR REPLACE VIEW VW_CUSTOMER_LIFETIME_VALUE AS
SELECT 
    c.CustomerID,
    c.OfficeID,
    c.FName || ' ' || c.LName as CustomerName,
    c.CompanyName,
    c.DateAdded,
    c.DateCancelled,
    c.Status,
    COUNT(DISTINCT t.TicketID) as TotalTickets,
    SUM(t.Total) as LifetimeRevenue,
    SUM(t.Balance) as OutstandingBalance,
    AVG(t.Total) as AvgTicketValue,
    DATEDIFF(month, c.DateAdded, COALESCE(c.DateCancelled, CURRENT_DATE())) as TenureMonths
FROM STAGING_DB.FIELDROUTES.FACT_CUSTOMER c
LEFT JOIN STAGING_DB.FIELDROUTES.FACT_TICKET t ON c.CustomerID = t.CustomerID
GROUP BY 
    c.CustomerID,
    c.OfficeID,
    c.FName,
    c.LName,
    c.CompanyName,
    c.DateAdded,
    c.DateCancelled,
    c.Status;

-- Subscription Performance View
CREATE OR REPLACE VIEW VW_SUBSCRIPTION_PERFORMANCE AS
SELECT 
    s.SubscriptionID,
    s.OfficeID,
    s.CustomerID,
    s.ServiceID,
    s.Frequency,
    s.DateAdded,
    s.NextServiceDate,
    s.SubscriptionStatus,
    s.AgreementLength,
    s.PreferredDays,
    s.PreferredEmployee,
    s.Price,
    s.RegionID,
    st.ServiceName,
    st.Category as ServiceCategory
FROM STAGING_DB.FIELDROUTES.FACT_SUBSCRIPTION s
LEFT JOIN STAGING_DB.FIELDROUTES.DIM_SERVICETYPE st ON s.ServiceID = st.ServiceTypeID AND s.OfficeID = st.OfficeID;

-- ===== GRANT PERMISSIONS =====
-- Grant SELECT on all views to appropriate roles
GRANT SELECT ON ALL VIEWS IN SCHEMA PRODUCTION_DB.FIELDROUTES TO ROLE ACCOUNTADMIN;

-- Create a read-only role for Power BI if needed
CREATE ROLE IF NOT EXISTS POWERBI_READER;
GRANT USAGE ON DATABASE PRODUCTION_DB TO ROLE POWERBI_READER;
GRANT USAGE ON SCHEMA PRODUCTION_DB.FIELDROUTES TO ROLE POWERBI_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA PRODUCTION_DB.FIELDROUTES TO ROLE POWERBI_READER;

-- Grant the role to specific users
-- GRANT ROLE POWERBI_READER TO USER 'powerbi_service_account';