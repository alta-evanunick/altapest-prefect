-- =====================================================
-- ALTA PEST CONTROL AR & REVENUE OPERATIONS DASHBOARD
-- Views specifically designed for the dashboard requirements
-- =====================================================

USE DATABASE RAW;
USE SCHEMA REPORTING;

-- =====================================================
-- AR AGING VIEW - Core for DSO and aging analysis
-- =====================================================
CREATE OR REPLACE VIEW VW_AR_AGING AS
WITH ticket_balance AS (
    SELECT 
        t.ticketID,
        t.customerID,
        t.officeID,
        t.invoiceDate,
        t.total AS invoiceAmount,
        t.balance AS currentBalance,
        t.serviceID,
        -- Calculate age in days
        DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) AS ageDays,
        -- Age buckets
        CASE 
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 30 THEN '0-30 Days'
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 60 THEN '31-60 Days'
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 90 THEN '61-90 Days'
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 120 THEN '91-120 Days'
            ELSE '120+ Days'
        END AS ageBucket,
        -- Numeric bucket for sorting
        CASE 
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 30 THEN 1
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 60 THEN 2
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 90 THEN 3
            WHEN DATEDIFF(day, t.invoiceDate, CURRENT_DATE()) <= 120 THEN 4
            ELSE 5
        END AS ageBucketSort
    FROM VW_TICKET t
    WHERE t.balance > 0  -- Only outstanding balances
      AND t.isActive = TRUE
)
SELECT 
    tb.*,
    c.fName,
    c.lName,
    c.companyName,
    c.regionID,
    c.phone1,
    c.email,
    c.address,
    c.city,
    c.state,
    c.status AS customerStatus,
    o.officeName,
    st.serviceTypeName,
    st.category AS serviceCategory
FROM ticket_balance tb
LEFT JOIN VW_CUSTOMER c ON tb.customerID = c.customerID
LEFT JOIN VW_DIM_OFFICE o ON tb.officeID = o.officeID
LEFT JOIN VW_DIM_SERVICE_TYPE st ON tb.serviceID = st.serviceTypeID;

-- =====================================================
-- DSO (Days Sales Outstanding) CALCULATION VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_DSO_METRICS AS
WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) AS reportMonth,
        officeID,
        officeName,
        -- Total AR
        SUM(currentBalance) AS totalAR,
        -- AR by age
        SUM(CASE WHEN ageDays <= 60 THEN currentBalance ELSE 0 END) AS arUnder60,
        SUM(CASE WHEN ageDays > 60 THEN currentBalance ELSE 0 END) AS arOver60,
        -- Count by age
        COUNT(CASE WHEN ageDays > 60 THEN 1 END) AS accountsOver60,
        COUNT(*) AS totalAccounts
    FROM VW_AR_AGING
    GROUP BY DATE_TRUNC('month', CURRENT_DATE()), officeID, officeName
),
revenue_metrics AS (
    SELECT 
        officeID,
        -- Last 90 days revenue for DSO calc
        SUM(appliedAmount) / 90.0 AS avgDailyRevenue
    FROM VW_APPLIED_PAYMENT
    WHERE dateApplied >= DATEADD(day, -90, CURRENT_DATE())
    GROUP BY officeID
)
SELECT 
    m.reportMonth,
    m.officeID,
    m.officeName,
    m.totalAR,
    m.arUnder60,
    m.arOver60,
    -- Percentage over 60 days
    CASE 
        WHEN m.totalAR > 0 
        THEN (m.arOver60 * 100.0) / m.totalAR 
        ELSE 0 
    END AS percentOver60Days,
    m.accountsOver60,
    m.totalAccounts,
    r.avgDailyRevenue,
    -- DSO Calculation
    CASE 
        WHEN r.avgDailyRevenue > 0 
        THEN m.totalAR / r.avgDailyRevenue
        ELSE NULL 
    END AS dso
FROM monthly_metrics m
LEFT JOIN revenue_metrics r ON m.officeID = r.officeID;

-- =====================================================
-- COLLECTION EFFICIENCY INDEX (CEI) VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_CEI_METRICS AS
WITH period_metrics AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) AS reportMonth,
        ap.officeID,
        -- Beginning AR (first day of month)
        SUM(CASE 
            WHEN t.invoiceDate < DATE_TRUNC('month', CURRENT_DATE()) 
            THEN t.balance 
            ELSE 0 
        END) AS beginningAR,
        -- Current month sales
        SUM(CASE 
            WHEN DATE_TRUNC('month', t.invoiceDate) = DATE_TRUNC('month', CURRENT_DATE()) 
            THEN t.total 
            ELSE 0 
        END) AS currentMonthSales,
        -- Collections this month
        SUM(CASE 
            WHEN DATE_TRUNC('month', ap.dateApplied) = DATE_TRUNC('month', CURRENT_DATE()) 
            THEN ap.appliedAmount 
            ELSE 0 
        END) AS monthlyCollections
    FROM VW_APPLIED_PAYMENT ap
    LEFT JOIN VW_TICKET t ON ap.ticketID = t.ticketID
    GROUP BY DATE_TRUNC('month', CURRENT_DATE()), ap.officeID
)
SELECT 
    reportMonth,
    officeID,
    beginningAR,
    currentMonthSales,
    monthlyCollections,
    beginningAR + currentMonthSales AS totalCollectable,
    -- CEI Calculation: (Collections / (Beginning AR + Sales)) * 100
    CASE 
        WHEN (beginningAR + currentMonthSales) > 0
        THEN (monthlyCollections * 100.0) / (beginningAR + currentMonthSales)
        ELSE 0
    END AS cei
FROM period_metrics;

-- =====================================================
-- AR WATERFALL VIEW - For executive overview
-- =====================================================
CREATE OR REPLACE VIEW VW_AR_WATERFALL AS
WITH daily_movements AS (
    SELECT 
        DATE_TRUNC('month', CURRENT_DATE()) AS reportMonth,
        officeID,
        -- Beginning AR (start of month)
        SUM(CASE 
            WHEN invoiceDate < DATE_TRUNC('month', CURRENT_DATE()) 
            THEN total - (total - balance)  -- Original invoice less payments before month
            ELSE 0 
        END) AS beginningAR,
        -- New sales this month
        SUM(CASE 
            WHEN DATE_TRUNC('month', invoiceDate) = DATE_TRUNC('month', CURRENT_DATE()) 
            THEN total 
            ELSE 0 
        END) AS newSales
    FROM VW_TICKET
    GROUP BY DATE_TRUNC('month', CURRENT_DATE()), officeID
),
collections AS (
    SELECT 
        DATE_TRUNC('month', dateApplied) AS reportMonth,
        officeID,
        SUM(appliedAmount) AS totalCollected
    FROM VW_APPLIED_PAYMENT
    WHERE DATE_TRUNC('month', dateApplied) = DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', dateApplied), officeID
),
writeoffs AS (
    SELECT 
        DATE_TRUNC('month', paymentDate) AS reportMonth,
        officeID,
        SUM(amount) AS totalWriteoffs
    FROM VW_PAYMENT
    WHERE writeoff = TRUE
      AND DATE_TRUNC('month', paymentDate) = DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', paymentDate), officeID
)
SELECT 
    d.reportMonth,
    d.officeID,
    d.beginningAR,
    d.newSales,
    COALESCE(c.totalCollected, 0) AS collected,
    COALESCE(w.totalWriteoffs, 0) AS writeoffs,
    -- Calculate ending AR
    d.beginningAR + d.newSales - COALESCE(c.totalCollected, 0) - COALESCE(w.totalWriteoffs, 0) AS endingAR
FROM daily_movements d
LEFT JOIN collections c ON d.officeID = c.officeID AND d.reportMonth = c.reportMonth
LEFT JOIN writeoffs w ON d.officeID = w.officeID AND d.reportMonth = w.reportMonth;

-- =====================================================
-- PAYMENT DECLINE ANALYSIS VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_PAYMENT_DECLINES AS
WITH payment_attempts AS (
    SELECT 
        p.officeID,
        p.customerID,
        p.paymentID,
        p.paymentDate,
        p.paymentMethod,
        p.amount,
        p.status,
        p.cardType,
        pp.failedAttempts,
        pp.lastDeclineType,
        pp.retryPoints,
        -- Categorize payment status
        CASE 
            WHEN p.status = 1 THEN 'Success'
            WHEN p.status = 2 THEN 'Declined'
            WHEN p.status = 3 THEN 'Pending'
            ELSE 'Other'
        END AS paymentStatus,
        -- Check if this was a retry that succeeded
        LAG(p.status) OVER (PARTITION BY p.customerID, p.paymentMethod ORDER BY p.paymentDate) AS previousStatus
    FROM VW_PAYMENT p
    LEFT JOIN (
        SELECT 
            customerID,
            MAX(failedAttempts) AS failedAttempts,
            MAX(lastDeclineType) AS lastDeclineType,
            MAX(retryPoints) AS retryPoints
        FROM RAW.FIELDROUTES.PAYMENTPROFILE_FACT
        GROUP BY customerID
    ) pp ON p.customerID = pp.customerID
)
SELECT 
    officeID,
    DATE_TRUNC('month', paymentDate) AS paymentMonth,
    paymentMethod,
    cardType,
    COUNT(*) AS totalAttempts,
    COUNT(CASE WHEN paymentStatus = 'Success' THEN 1 END) AS successfulPayments,
    COUNT(CASE WHEN paymentStatus = 'Declined' THEN 1 END) AS declinedPayments,
    COUNT(CASE WHEN paymentStatus = 'Success' AND previousStatus = 2 THEN 1 END) AS retrySuccesses,
    -- Calculate rates
    COUNT(CASE WHEN paymentStatus = 'Declined' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) AS declineRate,
    COUNT(CASE WHEN paymentStatus = 'Success' AND previousStatus = 2 THEN 1 END) * 100.0 / 
        NULLIF(COUNT(CASE WHEN previousStatus = 2 THEN 1 END), 0) AS retrySuccessRate,
    -- Revenue impact
    SUM(CASE WHEN paymentStatus = 'Success' THEN amount ELSE 0 END) AS successfulRevenue,
    SUM(CASE WHEN paymentStatus = 'Declined' THEN amount ELSE 0 END) AS declinedRevenue
FROM payment_attempts
GROUP BY officeID, DATE_TRUNC('month', paymentDate), paymentMethod, cardType;

-- =====================================================
-- COUPON/DISCOUNT IMPACT VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_REVENUE_LEAKAGE AS
WITH ticket_details AS (
    SELECT 
        t.officeID,
        t.ticketID,
        t.invoiceDate,
        t.serviceID,
        t.subtotal,
        t.total,
        t.serviceCharge,
        -- Calculate discount amount
        CASE 
            WHEN t.subtotal > t.total 
            THEN t.subtotal - t.total 
            ELSE 0 
        END AS discountAmount,
        st.serviceTypeName,
        st.category AS serviceCategory
    FROM VW_TICKET t
    LEFT JOIN VW_DIM_SERVICE_TYPE st ON t.serviceID = st.serviceTypeID
),
payment_data AS (
    SELECT 
        officeID,
        DATE_TRUNC('month', paymentDate) AS paymentMonth,
        SUM(CASE WHEN writeoff = TRUE THEN amount ELSE 0 END) AS writeoffAmount,
        SUM(CASE WHEN status = 2 THEN amount ELSE 0 END) AS declinedAmount
    FROM VW_PAYMENT
    GROUP BY officeID, DATE_TRUNC('month', paymentDate)
)
SELECT 
    td.officeID,
    DATE_TRUNC('month', td.invoiceDate) AS revenueMonth,
    td.serviceCategory,
    td.serviceTypeName,
    COUNT(DISTINCT td.ticketID) AS ticketCount,
    SUM(td.subtotal) AS grossRevenue,
    SUM(td.total) AS netRevenue,
    SUM(td.discountAmount) AS totalDiscounts,
    -- Discount percentage
    CASE 
        WHEN SUM(td.subtotal) > 0 
        THEN (SUM(td.discountAmount) * 100.0) / SUM(td.subtotal)
        ELSE 0 
    END AS discountPercentage,
    -- Add payment leakage data
    COALESCE(pd.writeoffAmount, 0) AS writeoffs,
    COALESCE(pd.declinedAmount, 0) AS declines,
    -- Total leakage
    SUM(td.discountAmount) + COALESCE(pd.writeoffAmount, 0) + COALESCE(pd.declinedAmount, 0) AS totalLeakage,
    -- Leakage percentage of gross sales
    CASE 
        WHEN SUM(td.subtotal) > 0 
        THEN ((SUM(td.discountAmount) + COALESCE(pd.writeoffAmount, 0) + COALESCE(pd.declinedAmount, 0)) * 100.0) / SUM(td.subtotal)
        ELSE 0 
    END AS leakagePercentage
FROM ticket_details td
LEFT JOIN payment_data pd 
    ON td.officeID = pd.officeID 
    AND DATE_TRUNC('month', td.invoiceDate) = pd.paymentMonth
GROUP BY td.officeID, DATE_TRUNC('month', td.invoiceDate), td.serviceCategory, td.serviceTypeName,
         pd.writeoffAmount, pd.declinedAmount;

-- =====================================================
-- COLLECTIONS AGENCY PERFORMANCE VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_AGENCY_PERFORMANCE AS
WITH agency_accounts AS (
    -- Identify accounts sent to collections (typically 120+ days)
    SELECT 
        customerID,
        officeID,
        SUM(currentBalance) AS placedAmount,
        MIN(invoiceDate) AS oldestInvoiceDate,
        COUNT(DISTINCT ticketID) AS placedInvoices
    FROM VW_AR_AGING
    WHERE ageDays > 120  -- Typical agency placement threshold
    GROUP BY customerID, officeID
),
agency_recoveries AS (
    SELECT 
        aa.customerID,
        aa.officeID,
        aa.placedAmount,
        aa.placedInvoices,
        -- Calculate recoveries after placement
        SUM(CASE 
            WHEN ap.dateApplied > DATEADD(day, 120, aa.oldestInvoiceDate) 
            THEN ap.appliedAmount 
            ELSE 0 
        END) AS recoveredAmount,
        COUNT(DISTINCT CASE 
            WHEN ap.dateApplied > DATEADD(day, 120, aa.oldestInvoiceDate) 
            THEN ap.paymentID 
        END) AS recoveryPayments
    FROM agency_accounts aa
    LEFT JOIN VW_APPLIED_PAYMENT ap ON aa.customerID = ap.customerID
    GROUP BY aa.customerID, aa.officeID, aa.placedAmount, aa.placedInvoices
)
SELECT 
    officeID,
    DATE_TRUNC('month', CURRENT_DATE()) AS reportMonth,
    COUNT(DISTINCT customerID) AS accountsPlaced,
    SUM(placedAmount) AS totalPlacedAmount,
    SUM(placedInvoices) AS totalPlacedInvoices,
    SUM(recoveredAmount) AS totalRecoveredAmount,
    SUM(recoveryPayments) AS totalRecoveryPayments,
    COUNT(DISTINCT CASE WHEN recoveredAmount > 0 THEN customerID END) AS accountsRecovered,
    -- Recovery rate by dollars
    CASE 
        WHEN SUM(placedAmount) > 0 
        THEN (SUM(recoveredAmount) * 100.0) / SUM(placedAmount)
        ELSE 0 
    END AS recoveryRateDollars,
    -- Recovery rate by accounts
    CASE 
        WHEN COUNT(DISTINCT customerID) > 0 
        THEN (COUNT(DISTINCT CASE WHEN recoveredAmount > 0 THEN customerID END) * 100.0) / COUNT(DISTINCT customerID)
        ELSE 0 
    END AS recoveryRateAccounts,
    -- Assume 25% agency fee (adjust as needed)
    SUM(recoveredAmount) * 0.75 AS netBackAfterFees
FROM agency_recoveries
GROUP BY officeID, DATE_TRUNC('month', CURRENT_DATE());

-- =====================================================
-- AR REP PRODUCTIVITY VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_AR_REP_PRODUCTIVITY AS
WITH daily_activity AS (
    SELECT 
        -- Note: This assumes you have call/contact data in the note entity
        n.employeeID,
        DATE(n.dateCreated) AS activityDate,
        COUNT(DISTINCT CASE WHEN n.typeID IN (1,2,3) THEN n.noteID END) AS totalDials,  -- Adjust type IDs
        COUNT(DISTINCT CASE WHEN n.typeID = 4 THEN n.noteID END) AS totalContacts,  -- Adjust type ID
        -- Get payments collected by this employee on this date
        COUNT(DISTINCT p.paymentID) AS paymentsCollected,
        SUM(p.amount) AS amountCollected,
        COUNT(DISTINCT CASE WHEN pp.autopayPaymentProfileID IS NOT NULL THEN p.paymentID END) AS autopayPayments
    FROM VW_NOTE n
    LEFT JOIN VW_PAYMENT p 
        ON n.employeeID = p.employeeID 
        AND DATE(n.dateCreated) = DATE(p.paymentDate)
    LEFT JOIN VW_CUSTOMER pp 
        ON p.customerID = pp.customerID
    WHERE n.dateCreated >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY n.employeeID, DATE(n.dateCreated)
)
SELECT 
    da.employeeID,
    e.fName,
    e.lName,
    da.activityDate,
    da.totalDials,
    da.totalContacts,
    -- Assume 8-hour workday for hourly calculations
    da.totalDials / 8.0 AS dialsPerHour,
    da.totalContacts / 8.0 AS contactsPerHour,
    -- Contact rate
    CASE 
        WHEN da.totalDials > 0 
        THEN (da.totalContacts * 100.0) / da.totalDials 
        ELSE 0 
    END AS contactRate,
    da.paymentsCollected,
    da.amountCollected,
    da.amountCollected / 8.0 AS collectedPerHour,
    -- Autopay percentage
    CASE 
        WHEN da.paymentsCollected > 0 
        THEN (da.autopayPayments * 100.0) / da.paymentsCollected
        ELSE 0 
    END AS autopayPercentage
FROM daily_activity da
LEFT JOIN VW_EMPLOYEE e ON da.employeeID = e.employeeID;

-- =====================================================
-- AR PROGRAM PERFORMANCE VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_AR_PROGRAM_PERFORMANCE AS
WITH program_metrics AS (
    SELECT 
        DATE_TRUNC('month', dateApplied) AS reportMonth,
        officeID,
        -- Daily metrics
        DATE(dateApplied) AS collectionDate,
        SUM(appliedAmount) AS dailyCollected,
        COUNT(DISTINCT customerID) AS customersCollected
    FROM VW_APPLIED_PAYMENT
    WHERE dateApplied >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', dateApplied), officeID, DATE(dateApplied)
),
available_ar AS (
    SELECT 
        officeID,
        DATE_TRUNC('month', CURRENT_DATE()) AS reportMonth,
        SUM(currentBalance) AS totalAvailableAR
    FROM VW_AR_AGING
    GROUP BY officeID
)
SELECT 
    pm.reportMonth,
    pm.officeID,
    -- Daily average
    AVG(pm.dailyCollected) AS avgDailyCollected,
    -- Weekly average (multiply daily by 5 for business days)
    AVG(pm.dailyCollected) * 5 AS avgWeeklyCollected,
    -- Monthly total
    SUM(pm.dailyCollected) AS monthlyCollected,
    -- Collection percentage of available
    CASE 
        WHEN aar.totalAvailableAR > 0 
        THEN (SUM(pm.dailyCollected) * 100.0) / aar.totalAvailableAR
        ELSE 0 
    END AS collectedPercentOfAvailable,
    aar.totalAvailableAR,
    -- ROI calculations would need wage data
    -- These are placeholders - adjust based on your wage tracking
    SUM(pm.dailyCollected) / 5000.0 AS variableROI,  -- Assume $5k monthly wages
    SUM(pm.dailyCollected) / 8000.0 AS totalROI      -- Assume $8k with overhead
FROM program_metrics pm
LEFT JOIN available_ar aar 
    ON pm.officeID = aar.officeID 
    AND pm.reportMonth = aar.reportMonth
GROUP BY pm.reportMonth, pm.officeID, aar.totalAvailableAR;

-- =====================================================
-- DATA QUALITY FLAGS VIEW
-- =====================================================
CREATE OR REPLACE VIEW VW_DATA_QUALITY_FLAGS AS
WITH customer_flags AS (
    SELECT 
        c.customerID,
        c.officeID,
        c.fName,
        c.lName,
        c.companyName,
        -- Bad address flag (missing or incomplete)
        CASE 
            WHEN c.address IS NULL OR c.city IS NULL OR c.state IS NULL OR c.zip IS NULL 
            THEN 1 ELSE 0 
        END AS badAddressFlag,
        -- Bad phone flag
        CASE 
            WHEN c.phone1 IS NULL OR LENGTH(REGEXP_REPLACE(c.phone1, '[^0-9]', '')) < 10 
            THEN 1 ELSE 0 
        END AS badPhoneFlag,
        -- Bad/No email flag
        CASE 
            WHEN c.email IS NULL OR c.email NOT LIKE '%@%.%' 
            THEN 1 ELSE 0 
        END AS badEmailFlag,
        -- Get payment decline info
        pd.declineCount,
        pd.maxConsecutiveDeclines,
        -- Age outlier (very old AR)
        ar.maxAgeDays,
        ar.totalBalance,
        -- Last service date
        svc.lastServiceDate,
        svc.daysSinceLastService,
        -- Cycling detection
        c.dateCancelled,
        c.dateAdded
    FROM VW_CUSTOMER c
    LEFT JOIN (
        SELECT 
            customerID,
            COUNT(CASE WHEN status = 2 THEN 1 END) AS declineCount,
            MAX(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS maxConsecutiveDeclines
        FROM VW_PAYMENT
        WHERE paymentDate >= DATEADD(month, -6, CURRENT_DATE())
        GROUP BY customerID
    ) pd ON c.customerID = pd.customerID
    LEFT JOIN (
        SELECT 
            customerID,
            MAX(ageDays) AS maxAgeDays,
            SUM(currentBalance) AS totalBalance
        FROM VW_AR_AGING
        GROUP BY customerID
    ) ar ON c.customerID = ar.customerID
    LEFT JOIN (
        SELECT 
            customerID,
            MAX(dateCompleted) AS lastServiceDate,
            DATEDIFF(day, MAX(dateCompleted), CURRENT_DATE()) AS daysSinceLastService
        FROM VW_APPOINTMENT
        WHERE status = 4  -- Completed
        GROUP BY customerID
    ) svc ON c.customerID = svc.customerID
)
SELECT 
    customerID,
    officeID,
    fName,
    lName,
    companyName,
    badAddressFlag,
    badPhoneFlag,
    badEmailFlag,
    -- Decline outlier (more than 3 declines in 6 months)
    CASE WHEN declineCount > 3 THEN 1 ELSE 0 END AS declineOutlierFlag,
    -- Age outlier (AR over 180 days)
    CASE WHEN maxAgeDays > 180 THEN 1 ELSE 0 END AS ageOutlierFlag,
    -- Unserviced for 90+ days but still active
    CASE 
        WHEN daysSinceLastService > 90 AND dateCancelled IS NULL 
        THEN 1 ELSE 0 
    END AS unservicedFlag,
    -- Account cycling (cancelled and restarted within a year)
    CASE 
        WHEN dateCancelled IS NOT NULL 
        AND EXISTS (
            SELECT 1 FROM VW_CUSTOMER c2 
            WHERE c2.customerID = customerID 
            AND c2.dateAdded > dateCancelled
            AND c2.dateAdded < DATEADD(year, 1, dateCancelled)
        )
        THEN 1 ELSE 0 
    END AS accountCyclingFlag,
    -- Summary quality score (0-7, lower is better)
    badAddressFlag + badPhoneFlag + badEmailFlag + 
    CASE WHEN declineCount > 3 THEN 1 ELSE 0 END +
    CASE WHEN maxAgeDays > 180 THEN 1 ELSE 0 END +
    CASE WHEN daysSinceLastService > 90 AND dateCancelled IS NULL THEN 1 ELSE 0 END AS dataQualityScore
FROM customer_flags;

-- =====================================================
-- DSO TREND VIEW - 12 month rolling
-- =====================================================
CREATE OR REPLACE VIEW VW_DSO_TREND AS
WITH monthly_ar AS (
    SELECT 
        DATE_TRUNC('month', DATEADD(month, -n.n, CURRENT_DATE())) AS reportMonth,
        t.officeID,
        t.serviceID,
        SUM(t.balance) AS monthEndAR
    FROM VW_TICKET t
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER (ORDER BY seq4()) - 1 AS n
        FROM TABLE(GENERATOR(ROWCOUNT => 12))
    ) n
    WHERE t.invoiceDate <= LAST_DAY(DATEADD(month, -n.n, CURRENT_DATE()))
      AND t.balance > 0
    GROUP BY DATE_TRUNC('month', DATEADD(month, -n.n, CURRENT_DATE())), t.officeID, t.serviceID
),
monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', dateApplied) AS revenueMonth,
        officeID,
        SUM(appliedAmount) AS monthlyRevenue,
        SUM(appliedAmount) / DAY(LAST_DAY(DATE_TRUNC('month', dateApplied))) AS avgDailyRevenue
    FROM VW_APPLIED_PAYMENT
    WHERE dateApplied >= DATEADD(month, -12, CURRENT_DATE())
    GROUP BY DATE_TRUNC('month', dateApplied), officeID
)
SELECT 
    mar.reportMonth,
    mar.officeID,
    o.officeName,
    mar.serviceID,
    st.serviceTypeName,
    st.category AS serviceCategory,
    mar.monthEndAR,
    mr.monthlyRevenue,
    mr.avgDailyRevenue,
    -- Calculate DSO
    CASE 
        WHEN mr.avgDailyRevenue > 0 
        THEN mar.monthEndAR / mr.avgDailyRevenue
        ELSE NULL 
    END AS dso
FROM monthly_ar mar
LEFT JOIN monthly_revenue mr 
    ON mar.officeID = mr.officeID 
    AND mar.reportMonth = mr.revenueMonth
LEFT JOIN VW_DIM_OFFICE o ON mar.officeID = o.officeID
LEFT JOIN VW_DIM_SERVICE_TYPE st ON mar.serviceID = st.serviceTypeID
WHERE mr.avgDailyRevenue > 0  -- Only where we have revenue to calculate DSO
ORDER BY mar.reportMonth DESC, mar.officeID;

-- =====================================================
-- Grant permissions for Power BI
-- =====================================================
-- GRANT SELECT ON ALL VIEWS IN SCHEMA REPORTING TO ROLE POWERBI_ROLE;