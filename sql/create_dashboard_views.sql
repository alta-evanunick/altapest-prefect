-- =====================================================
-- ALTA PEST CONTROL DASHBOARD VIEWS
-- Comprehensive views for Power BI dashboards
-- =====================================================

USE DATABASE RAW;
USE SCHEMA REPORTING;

-- =====================================================
-- APPLIED PAYMENT VIEW - Critical for revenue tracking
-- =====================================================
CREATE OR REPLACE VIEW VW_APPLIED_PAYMENT AS
WITH latest_records AS (
    -- AppliedPaymentID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:appliedPaymentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.APPLIEDPAYMENT_FACT
)
SELECT 
    -- Keys
    RawData:appliedPaymentID::INTEGER AS appliedPaymentID,
    RawData:officeID::INTEGER AS officeID,
    RawData:paymentID::INTEGER AS paymentID,
    RawData:ticketID::INTEGER AS ticketID,
    RawData:customerID::INTEGER AS customerID,
    
    -- Financial Details
    RawData:appliedAmount::FLOAT AS appliedAmount,
    RawData:taxCollected::FLOAT AS taxCollected,
    
    -- Employee & Dates
    RawData:appliedBy::INTEGER AS appliedByEmployeeID,
    RawData:dateApplied::TIMESTAMP_NTZ AS dateApplied,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Derived date fields for easier reporting
    DATE(RawData:dateApplied::TIMESTAMP_NTZ) AS dateAppliedDate,
    YEAR(RawData:dateApplied::TIMESTAMP_NTZ) AS appliedYear,
    MONTH(RawData:dateApplied::TIMESTAMP_NTZ) AS appliedMonth,
    QUARTER(RawData:dateApplied::TIMESTAMP_NTZ) AS appliedQuarter,
    WEEK(RawData:dateApplied::TIMESTAMP_NTZ) AS appliedWeek,
    DAYOFWEEK(RawData:dateApplied::TIMESTAMP_NTZ) AS appliedDayOfWeek,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- SUBSCRIPTION VIEW - For recurring revenue tracking
-- =====================================================
CREATE OR REPLACE VIEW VW_SUBSCRIPTION AS
WITH latest_records AS (
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:subscriptionID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.SUBSCRIPTION_FACT
)
SELECT 
    -- Keys
    RawData:subscriptionID::INTEGER AS subscriptionID,
    RawData:customerID::INTEGER AS customerID,
    RawData:officeID::INTEGER AS officeID,
    RawData:serviceTypeID::INTEGER AS serviceTypeID,
    
    -- Subscription Details
    RawData:active::BOOLEAN AS isActive,
    RawData:agreementLength::INTEGER AS agreementLength,
    RawData:agreementName::VARCHAR AS agreementName,
    RawData:billingFrequency::INTEGER AS billingFrequency,
    RawData:frequency::INTEGER AS frequency,
    RawData:followupService::BOOLEAN AS isFollowupService,
    
    -- Financial
    RawData:initialCharge::FLOAT AS initialCharge,
    RawData:recurringCharge::FLOAT AS recurringCharge,
    RawData:contractValue::FLOAT AS contractValue,
    
    -- Service Details
    RawData:lastCompleted::TIMESTAMP_NTZ AS lastCompletedDate,
    RawData:preferredTech::INTEGER AS preferredTechID,
    RawData:preferredStart::TIME AS preferredStartTime,
    RawData:preferredEnd::TIME AS preferredEndTime,
    RawData:duration::INTEGER AS serviceDuration,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS dateAdded,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    RawData:renewalDate::DATE AS renewalDate,
    RawData:nextService::DATE AS nextServiceDate,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- TICKETITEM VIEW - For detailed service line items
-- =====================================================
CREATE OR REPLACE VIEW VW_TICKET_ITEM AS
WITH latest_records AS (
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:ticketID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.TICKETITEM_FACT
)
SELECT 
    -- Keys
    RawData:ticketID::INTEGER AS ticketID,
    RawData:customerID::INTEGER AS customerID,
    RawData:billToAccountID::INTEGER AS billToAccountID,
    RawData:officeID::INTEGER AS officeID,
    RawData:appointmentID::INTEGER AS appointmentID,
    RawData:subscriptionID::INTEGER AS subscriptionID,
    RawData:serviceID::INTEGER AS serviceID,
    
    -- Ticket Information
    RawData:active::BOOLEAN AS isActive,
    RawData:invoiceDate::DATE AS ticketDate,
    RawData:glNumber::VARCHAR AS glNumber,
    RawData:createdBy::INTEGER AS createdByEmployeeID,
    
    -- Financial Details
    RawData:subtotal::FLOAT AS subtotal,
    RawData:taxAmount::FLOAT AS taxAmount,
    RawData:total::FLOAT AS total,
    RawData:serviceCharge::FLOAT AS serviceCharge,
    RawData:serviceTaxable::BOOLEAN AS serviceTaxable,
    RawData:productionValue::FLOAT AS productionValue,
    RawData:taxRate::FLOAT AS taxRate,
    RawData:balance::FLOAT AS remainingBalance,
    
    -- Line Items (flattened in separate view)
    RawData:items::ARRAY AS itemArray,
    
    -- Dates
    RawData:dateCreated::TIMESTAMP_NTZ AS dateCreated,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- TICKET ITEM DETAILS VIEW - Flattened line items
-- =====================================================
CREATE OR REPLACE VIEW VW_TICKET_ITEM_DETAILS AS
SELECT 
    t.ticketID,
    t.customerID,
    t.officeID,
    t.ticketDate,
    f.index AS lineNumber,
    f.value:description::VARCHAR AS itemDescription,
    f.value:quantity::INTEGER AS quantity,
    f.value:rate::FLOAT AS rate,
    f.value:amount::FLOAT AS amount,
    f.value:taxable::BOOLEAN AS isTaxable,
    f.value:productID::INTEGER AS productID,
    f.value:glAccountID::INTEGER AS glAccountID
FROM VW_TICKET_ITEM t,
LATERAL FLATTEN(input => t.itemArray, OUTER => TRUE) f
WHERE f.value IS NOT NULL;

-- =====================================================
-- DAILY REVENUE SUMMARY - For revenue dashboards
-- =====================================================
CREATE OR REPLACE VIEW VW_DAILY_REVENUE_SUMMARY AS
SELECT 
    ap.officeID,
    ap.dateAppliedDate AS revenueDate,
    COUNT(DISTINCT ap.customerID) AS uniqueCustomers,
    COUNT(DISTINCT ap.paymentID) AS paymentCount,
    COUNT(ap.appliedPaymentID) AS applicationCount,
    SUM(ap.appliedAmount) AS totalRevenue,
    SUM(ap.taxCollected) AS totalTax,
    SUM(ap.appliedAmount - ap.taxCollected) AS netRevenue
FROM VW_APPLIED_PAYMENT ap
GROUP BY ap.officeID, ap.dateAppliedDate;

-- =====================================================
-- CUSTOMER LIFETIME VALUE VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_CUSTOMER_LIFETIME_VALUE AS
SELECT 
    c.customerID,
    c.officeID,
    c.fName,
    c.lName,
    c.companyName,
    c.dateAdded,
    c.dateCancelled,
    c.status,
    c.regionID,
    -- Customer tenure
    DATEDIFF(day, c.dateAdded, COALESCE(c.dateCancelled, CURRENT_DATE())) AS customerTenureDays,
    DATEDIFF(month, c.dateAdded, COALESCE(c.dateCancelled, CURRENT_DATE())) AS customerTenureMonths,
    -- Revenue metrics
    COALESCE(rev.totalRevenue, 0) AS lifetimeRevenue,
    COALESCE(rev.paymentCount, 0) AS lifetimePayments,
    COALESCE(rev.firstPaymentDate, c.dateAdded) AS firstPaymentDate,
    COALESCE(rev.lastPaymentDate, c.dateAdded) AS lastPaymentDate,
    -- Average monthly revenue
    CASE 
        WHEN DATEDIFF(month, c.dateAdded, COALESCE(c.dateCancelled, CURRENT_DATE())) > 0
        THEN COALESCE(rev.totalRevenue, 0) / DATEDIFF(month, c.dateAdded, COALESCE(c.dateCancelled, CURRENT_DATE()))
        ELSE 0
    END AS avgMonthlyRevenue,
    -- Service metrics
    COALESCE(svc.completedServices, 0) AS completedServices,
    COALESCE(svc.cancelledServices, 0) AS cancelledServices,
    svc.lastServiceDate
FROM VW_CUSTOMER c
LEFT JOIN (
    SELECT 
        customerID,
        SUM(appliedAmount) AS totalRevenue,
        COUNT(DISTINCT paymentID) AS paymentCount,
        MIN(dateApplied) AS firstPaymentDate,
        MAX(dateApplied) AS lastPaymentDate
    FROM VW_APPLIED_PAYMENT
    GROUP BY customerID
) rev ON c.customerID = rev.customerID
LEFT JOIN (
    SELECT 
        customerID,
        COUNT(CASE WHEN status = 4 THEN 1 END) AS completedServices,
        COUNT(CASE WHEN status = 6 THEN 1 END) AS cancelledServices,
        MAX(CASE WHEN status = 4 THEN dateCompleted END) AS lastServiceDate
    FROM VW_APPOINTMENT
    GROUP BY customerID
) svc ON c.customerID = svc.customerID;

-- =====================================================
-- TECHNICIAN PRODUCTIVITY VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_TECHNICIAN_PRODUCTIVITY AS
SELECT 
    a.employeeID,
    e.fName AS techFirstName,
    e.lName AS techLastName,
    a.appointmentDate,
    COUNT(DISTINCT a.appointmentID) AS appointmentCount,
    COUNT(DISTINCT CASE WHEN a.status = 4 THEN a.appointmentID END) AS completedAppointments,
    COUNT(DISTINCT CASE WHEN a.status = 6 THEN a.appointmentID END) AS cancelledAppointments,
    COUNT(DISTINCT a.customerID) AS uniqueCustomers,
    SUM(a.duration) AS totalScheduledMinutes,
    AVG(a.duration) AS avgScheduledMinutes,
    -- Calculate revenue generated
    SUM(t.serviceCharge) AS totalServiceCharges,
    SUM(t.productionValue) AS totalProductionValue
FROM VW_APPOINTMENT a
LEFT JOIN VW_EMPLOYEE e ON a.employeeID = e.employeeID
LEFT JOIN VW_TICKET t ON a.ticketID = t.ticketID
WHERE a.employeeID IS NOT NULL
GROUP BY a.employeeID, e.fName, e.lName, a.appointmentDate;

-- =====================================================
-- ROUTE EFFICIENCY VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_ROUTE_EFFICIENCY AS
SELECT 
    r.routeID,
    r.routeDate,
    r.officeID,
    r.assignedTech,
    r.routeTitle,
    r.totalDistance,
    r.appointmentDurationEstimate,
    r.drivingDurationEstimate,
    -- Count appointments on this route
    COUNT(DISTINCT a.appointmentID) AS appointmentCount,
    COUNT(DISTINCT CASE WHEN a.status = 4 THEN a.appointmentID END) AS completedCount,
    COUNT(DISTINCT a.customerID) AS uniqueCustomers,
    -- Calculate efficiency metrics
    CASE 
        WHEN COUNT(DISTINCT a.appointmentID) > 0 
        THEN r.totalDistance / COUNT(DISTINCT a.appointmentID)
        ELSE NULL 
    END AS avgDistancePerStop,
    -- Actual vs estimated
    SUM(DATEDIFF(minute, a.timeIn, a.timeOut)) AS actualServiceMinutes,
    r.appointmentDurationEstimate AS estimatedServiceMinutes
FROM VW_ROUTE r
LEFT JOIN VW_APPOINTMENT a ON r.routeID = a.routeID AND r.routeDate = a.appointmentDate
GROUP BY r.routeID, r.routeDate, r.officeID, r.assignedTech, r.routeTitle,
         r.totalDistance, r.appointmentDurationEstimate, r.drivingDurationEstimate;

-- =====================================================
-- MONTHLY RECURRING REVENUE (MRR) VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_MONTHLY_RECURRING_REVENUE AS
WITH subscription_mrr AS (
    SELECT 
        s.officeID,
        DATE_TRUNC('month', CURRENT_DATE()) AS mrrMonth,
        s.customerID,
        s.subscriptionID,
        s.isActive,
        s.recurringCharge,
        s.billingFrequency,
        -- Convert to monthly equivalent
        CASE 
            WHEN s.billingFrequency = 1 THEN s.recurringCharge  -- Monthly
            WHEN s.billingFrequency = 3 THEN s.recurringCharge / 3  -- Quarterly
            WHEN s.billingFrequency = 6 THEN s.recurringCharge / 6  -- Semi-annually
            WHEN s.billingFrequency = 12 THEN s.recurringCharge / 12  -- Annually
            ELSE s.recurringCharge  -- Default to monthly
        END AS monthlyValue
    FROM VW_SUBSCRIPTION s
    WHERE s.isActive = TRUE
)
SELECT 
    officeID,
    mrrMonth,
    COUNT(DISTINCT customerID) AS activeCustomers,
    COUNT(DISTINCT subscriptionID) AS activeSubscriptions,
    SUM(monthlyValue) AS totalMRR,
    AVG(monthlyValue) AS avgMRR,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthlyValue) AS medianMRR
FROM subscription_mrr
GROUP BY officeID, mrrMonth;

-- =====================================================
-- SERVICE TYPE PERFORMANCE VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_SERVICE_TYPE_PERFORMANCE AS
SELECT 
    st.serviceTypeID,
    st.officeID,
    st.description AS serviceTypeName,
    st.category,
    st.defaultCharge,
    -- Count appointments
    COUNT(DISTINCT a.appointmentID) AS appointmentCount,
    COUNT(DISTINCT CASE WHEN a.status = 4 THEN a.appointmentID END) AS completedCount,
    COUNT(DISTINCT a.customerID) AS uniqueCustomers,
    -- Revenue metrics
    SUM(t.serviceCharge) AS totalRevenue,
    AVG(t.serviceCharge) AS avgServiceCharge,
    -- Compare to default
    AVG(t.serviceCharge) - st.defaultCharge AS avgVarianceFromDefault,
    -- Completion rate
    CASE 
        WHEN COUNT(a.appointmentID) > 0 
        THEN COUNT(CASE WHEN a.status = 4 THEN 1 END) * 100.0 / COUNT(a.appointmentID)
        ELSE 0 
    END AS completionRate
FROM VW_DIM_SERVICE_TYPE st
LEFT JOIN VW_APPOINTMENT a ON st.serviceTypeID = a.appointmentType
LEFT JOIN VW_TICKET t ON a.ticketID = t.ticketID
GROUP BY st.serviceTypeID, st.officeID, st.description, st.category, st.defaultCharge;

-- =====================================================
-- CUSTOMER ACQUISITION VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_CUSTOMER_ACQUISITION AS
SELECT 
    c.officeID,
    DATE_TRUNC('month', c.dateAdded) AS acquisitionMonth,
    c.sourceID,
    c.source,
    cs.sourceName,
    COUNT(DISTINCT c.customerID) AS newCustomers,
    -- Calculate conversion to revenue
    COUNT(DISTINCT CASE WHEN rev.customerID IS NOT NULL THEN c.customerID END) AS customersWithRevenue,
    -- Average time to first payment
    AVG(DATEDIFF(day, c.dateAdded, rev.firstPaymentDate)) AS avgDaysToFirstPayment,
    -- First month revenue
    SUM(rev.firstMonthRevenue) AS totalFirstMonthRevenue,
    AVG(rev.firstMonthRevenue) AS avgFirstMonthRevenue
FROM VW_CUSTOMER c
LEFT JOIN VW_DIM_CUSTOMER_SOURCE cs ON c.sourceID = cs.sourceID
LEFT JOIN (
    SELECT 
        customerID,
        MIN(dateApplied) AS firstPaymentDate,
        SUM(CASE 
            WHEN DATEDIFF(month, MIN(dateApplied) OVER (PARTITION BY customerID), dateApplied) = 0 
            THEN appliedAmount 
            ELSE 0 
        END) AS firstMonthRevenue
    FROM VW_APPLIED_PAYMENT
    GROUP BY customerID, dateApplied
) rev ON c.customerID = rev.customerID
GROUP BY c.officeID, DATE_TRUNC('month', c.dateAdded), c.sourceID, c.source, cs.sourceName;

-- =====================================================
-- PAYMENT METHOD ANALYSIS VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_PAYMENT_METHOD_ANALYSIS AS
SELECT 
    p.officeID,
    DATE_TRUNC('month', p.paymentDate) AS paymentMonth,
    p.paymentMethod,
    CASE p.paymentMethod
        WHEN 1 THEN 'Cash'
        WHEN 2 THEN 'Check'
        WHEN 3 THEN 'Credit Card'
        WHEN 4 THEN 'ACH'
        ELSE 'Other'
    END AS paymentMethodName,
    COUNT(DISTINCT p.paymentID) AS paymentCount,
    COUNT(DISTINCT p.customerID) AS uniqueCustomers,
    SUM(p.amount) AS totalAmount,
    SUM(p.appliedAmount) AS totalApplied,
    AVG(p.amount) AS avgPaymentAmount,
    -- Calculate processing metrics
    COUNT(CASE WHEN p.status = 1 THEN 1 END) AS successfulPayments,
    COUNT(CASE WHEN p.status = 2 THEN 1 END) AS failedPayments,
    COUNT(CASE WHEN p.writeoff = TRUE THEN 1 END) AS writeoffs
FROM VW_PAYMENT p
GROUP BY p.officeID, DATE_TRUNC('month', p.paymentDate), p.paymentMethod;

-- =====================================================
-- CANCELLATION ANALYSIS VIEW  
-- =====================================================
CREATE OR REPLACE VIEW VW_CANCELLATION_ANALYSIS AS
SELECT 
    ccr.OfficeID,
    DATE_TRUNC('month', ccr.DateCancelled) AS cancellationMonth,
    ccr.CancellationReasonID,
    ccr.CancellationReasonDescription,
    COUNT(DISTINCT ccr.CustomerID) AS cancelledCustomers,
    AVG(ccr.customerLifetimeDays) AS avgLifetimeDays,
    -- Get revenue metrics for cancelled customers
    SUM(clv.lifetimeRevenue) AS totalLostRevenue,
    AVG(clv.lifetimeRevenue) AS avgCustomerValue,
    AVG(clv.avgMonthlyRevenue) AS avgMonthlyRevenueLost
FROM VW_CUSTOMER_CANCELLATION_REASONS ccr
LEFT JOIN VW_CUSTOMER_LIFETIME_VALUE clv ON ccr.CustomerID = clv.customerID
GROUP BY ccr.OfficeID, DATE_TRUNC('month', ccr.DateCancelled), 
         ccr.CancellationReasonID, ccr.CancellationReasonDescription;

-- =====================================================
-- Grant permissions for Power BI
-- =====================================================
-- GRANT SELECT ON ALL VIEWS IN SCHEMA REPORTING TO ROLE POWERBI_ROLE;

-- =====================================================
-- Create indexes for performance (if needed)
-- =====================================================
-- Consider creating clustering keys on large fact tables by date columns