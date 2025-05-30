-- Create Reporting Schema for Power BI Integration
-- This creates flattened views from the raw JSON data

USE DATABASE RAW;
CREATE SCHEMA IF NOT EXISTS REPORTING;
USE SCHEMA REPORTING;

-- =====================================================
-- CUSTOMER VIEW - Core customer information
-- =====================================================
CREATE OR REPLACE VIEW VW_CUSTOMER AS
WITH latest_records AS (
    -- Get only the most recent record for each customer
    -- CustomerID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:customerID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.CUSTOMER_FACT
)
SELECT 
    -- Keys
    OfficeID,
    RawData:customerID::INTEGER AS CustomerID,
    
    -- Basic Information
    RawData:firstName::VARCHAR AS FirstName,
    RawData:lastName::VARCHAR AS LastName,
    RawData:companyName::VARCHAR AS CompanyName,
    RawData:commercialAccount::BOOLEAN AS IsCommercial,
    RawData:status::VARCHAR AS Status,
    RawData:statusText::VARCHAR AS StatusText,
    
    -- Contact Information
    RawData:email::VARCHAR AS Email,
    RawData:phoneNumber::VARCHAR AS PhoneNumber,
    RawData:mobileNumber::VARCHAR AS MobileNumber,
    
    -- Address Information
    RawData:address::VARCHAR AS Address,
    RawData:city::VARCHAR AS City,
    RawData:state::VARCHAR AS State,
    RawData:zip::VARCHAR AS ZipCode,
    RawData:latitude::FLOAT AS Latitude,
    RawData:longitude::FLOAT AS Longitude,
    
    -- Business Information
    RawData:balance::FLOAT AS Balance,
    RawData:balanceAge::INTEGER AS BalanceAge,
    RawData:customerSource::INTEGER AS CustomerSourceID,
    RawData:regionID::INTEGER AS RegionID,
    
    -- Important Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS DateAdded,
    RawData:dateCancelled::TIMESTAMP_NTZ AS DateCancelled,
    RawData:dateUpdated::TIMESTAMP_NTZ AS DateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS LastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- EMPLOYEE VIEW - Employee/Technician information
-- =====================================================
CREATE OR REPLACE VIEW VW_EMPLOYEE AS
WITH latest_records AS (
    -- EmployeeID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:employeeID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.EMPLOYEE_FACT
)
SELECT 
    -- Keys
    OfficeID,
    RawData:employeeID::INTEGER AS EmployeeID,
    
    -- Basic Information
    RawData:firstName::VARCHAR AS FirstName,
    RawData:lastName::VARCHAR AS LastName,
    RawData:nickname::VARCHAR AS Nickname,
    CONCAT(COALESCE(RawData:firstName::VARCHAR, ''), ' ', COALESCE(RawData:lastName::VARCHAR, '')) AS FullName,
    
    -- Employment Details
    RawData:type::INTEGER AS EmployeeType,
    RawData:typeText::VARCHAR AS EmployeeTypeText,
    RawData:active::BOOLEAN AS IsActive,
    RawData:username::VARCHAR AS Username,
    
    -- Contact Information
    RawData:email::VARCHAR AS Email,
    RawData:phone::VARCHAR AS PhoneNumber,
    RawData:mobilePhone::VARCHAR AS MobileNumber,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS DateAdded,
    RawData:dateUpdated::TIMESTAMP_NTZ AS DateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS LastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- APPOINTMENT VIEW - Service appointment details
-- =====================================================
CREATE OR REPLACE VIEW VW_APPOINTMENT AS
WITH latest_records AS (
    -- AppointmentID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:appointmentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.APPOINTMENT_FACT
)
SELECT 
    -- Keys
    OfficeID,
    RawData:appointmentID::INTEGER AS AppointmentID,
    RawData:customerID::INTEGER AS CustomerID,
    RawData:employeeID::INTEGER AS EmployeeID,
    RawData:routeID::INTEGER AS RouteID,
    RawData:subscriptionID::INTEGER AS SubscriptionID,
    
    -- Appointment Details
    RawData:type::INTEGER AS AppointmentType,
    RawData:typeText::VARCHAR AS AppointmentTypeText,
    RawData:status::INTEGER AS Status,
    RawData:statusText::VARCHAR AS StatusText,
    RawData:duration::INTEGER AS DurationMinutes,
    
    -- Scheduling
    RawData:date::DATE AS AppointmentDate,
    RawData:start::TIME AS StartTime,
    RawData:end::TIME AS EndTime,
    RawData:timeWindow::VARCHAR AS TimeWindow,
    RawData:callAhead::INTEGER AS CallAheadMinutes,
    
    -- Service Details
    RawData:servicedBy::INTEGER AS ServicedByEmployeeID,
    RawData:dateCompleted::TIMESTAMP_NTZ AS DateCompleted,
    RawData:completedDuration::INTEGER AS CompletedDurationMinutes,
    
    -- Financial
    RawData:serviceCharge::FLOAT AS ServiceCharge,
    RawData:tipAmount::FLOAT AS TipAmount,
    
    -- Notes
    RawData:notes::VARCHAR AS Notes,
    RawData:officeNotes::VARCHAR AS OfficeNotes,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS DateAdded,
    RawData:dateUpdated::TIMESTAMP_NTZ AS DateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS LastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- TICKET VIEW - Service ticket information
-- =====================================================
CREATE OR REPLACE VIEW VW_TICKET AS
WITH latest_records AS (
    -- TicketID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:ticketID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.TICKET_FACT
)
SELECT 
    -- Keys
    OfficeID,
    RawData:ticketID::INTEGER AS TicketID,
    RawData:customerID::INTEGER AS CustomerID,
    RawData:appointmentID::INTEGER AS AppointmentID,
    RawData:subscriptionID::INTEGER AS SubscriptionID,
    RawData:serviceID::INTEGER AS ServiceID,
    RawData:invoiceID::INTEGER AS InvoiceID,
    
    -- Ticket Details
    RawData:ticketNumber::VARCHAR AS TicketNumber,
    RawData:type::INTEGER AS TicketType,
    RawData:typeText::VARCHAR AS TicketTypeText,
    RawData:status::INTEGER AS Status,
    RawData:statusText::VARCHAR AS StatusText,
    
    -- Financial
    RawData:subTotal::FLOAT AS SubTotal,
    RawData:taxAmount::FLOAT AS TaxAmount,
    RawData:total::FLOAT AS Total,
    RawData:balance::FLOAT AS Balance,
    
    -- Service Details
    RawData:serviceDate::DATE AS ServiceDate,
    RawData:servicedBy::INTEGER AS ServicedByEmployeeID,
    RawData:serviceType::INTEGER AS ServiceTypeID,
    
    -- Dates
    RawData:dateCreated::TIMESTAMP_NTZ AS DateCreated,
    RawData:dateUpdated::TIMESTAMP_NTZ AS DateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS LastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- PAYMENT VIEW - Payment transactions
-- =====================================================
CREATE OR REPLACE VIEW VW_PAYMENT AS
WITH latest_records AS (
    -- PaymentID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:paymentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.PAYMENT_FACT
)
SELECT 
    -- Keys
    OfficeID,
    RawData:paymentID::INTEGER AS PaymentID,
    RawData:customerID::INTEGER AS CustomerID,
    
    -- Payment Details
    RawData:amount::FLOAT AS Amount,
    RawData:paymentMethod::INTEGER AS PaymentMethod,
    RawData:paymentMethodText::VARCHAR AS PaymentMethodText,
    RawData:status::INTEGER AS Status,
    RawData:statusText::VARCHAR AS StatusText,
    
    -- References
    RawData:checkNumber::VARCHAR AS CheckNumber,
    RawData:referenceNumber::VARCHAR AS ReferenceNumber,
    
    -- Processing
    RawData:processedBy::INTEGER AS ProcessedByEmployeeID,
    RawData:processingResponse::VARCHAR AS ProcessingResponse,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS DateAdded,
    RawData:dateProcessed::TIMESTAMP_NTZ AS DateProcessed,
    RawData:dateUpdated::TIMESTAMP_NTZ AS DateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS LastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- DIMENSION VIEWS - Reference data for lookups
-- =====================================================

-- Office Dimension
CREATE OR REPLACE VIEW VW_DIM_OFFICE AS
WITH latest_records AS (
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY OfficeID ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.OFFICE_DIM
)
SELECT 
    OfficeID,
    RawData:officeName::VARCHAR AS OfficeName,
    RawData:abbreviation::VARCHAR AS OfficeAbbreviation,
    RawData:address::VARCHAR AS Address,
    RawData:city::VARCHAR AS City,
    RawData:state::VARCHAR AS State,
    RawData:zip::VARCHAR AS ZipCode,
    RawData:phone::VARCHAR AS PhoneNumber,
    RawData:active::BOOLEAN AS IsActive
FROM latest_records
WHERE rn = 1;

-- Region Dimension
CREATE OR REPLACE VIEW VW_DIM_REGION AS
WITH latest_records AS (
    -- RegionID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:regionID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.REGION_DIM
)
SELECT 
    OfficeID,
    RawData:regionID::INTEGER AS RegionID,
    RawData:regionName::VARCHAR AS RegionName,
    RawData:active::BOOLEAN AS IsActive
FROM latest_records
WHERE rn = 1;

-- Service Type Dimension
CREATE OR REPLACE VIEW VW_DIM_SERVICE_TYPE AS
WITH latest_records AS (
    -- ServiceTypeID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:serviceTypeID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.SERVICETYPE_DIM
)
SELECT 
    OfficeID,
    RawData:serviceTypeID::INTEGER AS ServiceTypeID,
    RawData:description::VARCHAR AS ServiceTypeName,
    RawData:active::BOOLEAN AS IsActive
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- FACT AGGREGATION VIEW - Customer Cancellation Reasons
-- =====================================================
CREATE OR REPLACE VIEW VW_CUSTOMER_CANCELLATION_REASONS AS
SELECT 
    ccr.OfficeID,
    ccr.CustomerID,
    ccr.CancellationReasonID,
    ccr.CancellationReasonDescription,
    ccr.DateCancelled,
    c.FirstName,
    c.LastName,
    c.CompanyName,
    c.RegionID,
    DATEDIFF(day, c.DateAdded, ccr.DateCancelled) AS CustomerLifetimeDays
FROM RAW.FIELDROUTES.CUSTOMERCANCELLATIONREASONS_FACT ccr
LEFT JOIN VW_CUSTOMER c 
    ON ccr.OfficeID = c.OfficeID 
    AND ccr.CustomerID = c.CustomerID;

-- =====================================================
-- Grant permissions for Power BI service account
-- =====================================================
-- GRANT SELECT ON ALL VIEWS IN SCHEMA REPORTING TO ROLE POWERBI_ROLE;

-- =====================================================
-- Test the views
-- =====================================================
-- SELECT COUNT(*) AS customer_count FROM VW_CUSTOMER;
-- SELECT COUNT(*) AS employee_count FROM VW_EMPLOYEE;
-- SELECT COUNT(*) AS appointment_count FROM VW_APPOINTMENT;
-- SELECT * FROM VW_CUSTOMER LIMIT 10;