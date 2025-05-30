-- Create Reporting Schema for Power BI Integration
-- Version 2: Using field mappings from FR_SF_Lookup.csv

USE DATABASE RAW;
CREATE SCHEMA IF NOT EXISTS REPORTING;
USE SCHEMA REPORTING;

-- =====================================================
-- CUSTOMER VIEW - Core customer information
-- =====================================================
CREATE OR REPLACE VIEW VW_CUSTOMER AS
WITH latest_records AS (
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
    RawData:customerID::INTEGER AS customerID,
    RawData:billToAccountID::INTEGER AS billToAccountID,
    RawData:officeID::INTEGER AS officeID,
    
    -- Name Information
    RawData:fname::VARCHAR AS fName,
    RawData:lname::VARCHAR AS lName,
    RawData:companyName::VARCHAR AS companyName,
    RawData:spouse::VARCHAR AS spouse,
    
    -- Status
    RawData:commercialAccount::BOOLEAN AS isCommercial,
    RawData:status::INTEGER AS status,
    RawData:statusText::VARCHAR AS statusText,
    
    -- Contact Information
    RawData:email::VARCHAR AS email,
    RawData:phone1::VARCHAR AS phone1,
    RawData:ext1::VARCHAR AS phone1_ext,
    RawData:phone2::VARCHAR AS phone2,
    RawData:ext2::VARCHAR AS phone2_ext,
    
    -- Service Address
    RawData:address::VARCHAR AS address,
    RawData:city::VARCHAR AS city,
    RawData:state::VARCHAR AS state,
    RawData:zip::VARCHAR AS zip,
    RawData:county::VARCHAR AS county,
    RawData:lat::FLOAT AS latitude,
    RawData:lng::FLOAT AS longitude,
    RawData:squarefeet::INTEGER AS sqFeet,
    
    -- Billing Address
    RawData:billingCompanyName::VARCHAR AS billingCompanyName,
    RawData:billingfname::VARCHAR AS billingFName,
    RawData:billinglname::VARCHAR AS billingLName,
    RawData:billingaddress::VARCHAR AS billingAddress,
    RawData:billingcity::VARCHAR AS billingCity,
    RawData:billingstate::VARCHAR AS billingState,
    RawData:billingzip::VARCHAR AS billingZip,
    RawData:billingphone::VARCHAR AS billingPhone,
    RawData:billingemail::VARCHAR AS billingEmail,
    
    -- Financial Information
    RawData:balance::FLOAT AS balance,
    RawData:balanceAge::INTEGER AS balanceAge,
    RawData:responsibleBalance::FLOAT AS responsibleBalance,
    RawData:responsibleBalanceAge::INTEGER AS responsibleBalanceAge,
    RawData:aPay::FLOAT AS aPay,
    RawData:paidInFull::BOOLEAN AS paidInFull,
    RawData:maxMonthlyCharge::FLOAT AS maxMonthlyCharge,
    RawData:preferredBillingDate::INTEGER AS preferredBillingDate,
    RawData:paymentHoldDate::DATE AS paymentHoldDate,
    RawData:autopayPaymentProfileID::INTEGER AS autopayPaymentProfileID,
    
    -- Source & Region
    RawData:sourceID::INTEGER AS sourceID,
    RawData:source::VARCHAR AS source,
    RawData:customerSourceID::INTEGER AS customerSourceID,
    RawData:customerSource::VARCHAR AS customerSource,
    RawData:regionID::INTEGER AS regionID,
    RawData:divisionID::INTEGER AS divisionID,
    
    -- Employee References
    RawData:addedByID::INTEGER AS addedByEmployeeID,
    RawData:preferredTechID::INTEGER AS preferredTechID,
    
    -- Tax Information
    RawData:taxRate::FLOAT AS taxRate,
    RawData:stateTax::FLOAT AS stateTax,
    RawData:cityTax::FLOAT AS cityTax,
    RawData:countyTax::FLOAT AS countyTax,
    RawData:districtTax::FLOAT AS districtTax,
    RawData:districtTax1::FLOAT AS districtTax1,
    RawData:districtTax2::FLOAT AS districtTax2,
    RawData:districtTax3::FLOAT AS districtTax3,
    RawData:districtTax4::FLOAT AS districtTax4,
    RawData:districtTax5::FLOAT AS districtTax5,
    RawData:customTax::FLOAT AS customTax,
    RawData:zipTaxID::INTEGER AS zipTaxID,
    
    -- Communication Preferences
    RawData:smsReminders::BOOLEAN AS smsReminders,
    RawData:phoneReminders::BOOLEAN AS phoneReminders,
    RawData:emailReminders::BOOLEAN AS emailReminders,
    
    -- Credit Card Information
    RawData:mostRecentCreditCardLastFour::VARCHAR AS latestCCLastFour,
    RawData:mostRecentCreditCardExpirationDate::VARCHAR AS latestCCExpDate,
    
    -- Arrays (as JSON)
    RawData:subscriptionIDs::ARRAY AS subscriptionIDs,
    RawData:appointmentIDs::ARRAY AS appointmentIDs,
    RawData:ticketIDs::ARRAY AS ticketIDs,
    RawData:paymentIDs::ARRAY AS paymentIDs,
    
    -- Other Fields
    RawData:masterAccount::VARCHAR AS masterAccount,
    RawData:specialScheduling::VARCHAR AS specialScheduling,
    RawData:salesmanAPay::FLOAT AS salesmanAPay,
    RawData:termiteMonitoring::BOOLEAN AS termiteMonitoring,
    RawData:pendingCancel::BOOLEAN AS pendingCancel,
    
    -- Important Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS dateAdded,
    RawData:dateCancelled::TIMESTAMP_NTZ AS dateCancelled,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    RawData:agingDate::DATE AS agingDate,
    RawData:responsibleAgingDate::DATE AS responsibleAgingDate,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
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
    RawData:employeeID::INTEGER AS employeeID,
    RawData:officeID::INTEGER AS officeID,
    
    -- Basic Information
    RawData:fname::VARCHAR AS fName,
    RawData:lname::VARCHAR AS lName,
    RawData:active::BOOLEAN AS isActive,
    RawData:type::VARCHAR AS employeeTypeText,
    
    -- Contact Information
    RawData:phone::VARCHAR AS phone,
    RawData:email::VARCHAR AS email,
    
    -- Professional Information
    RawData:experience::VARCHAR AS experience,
    RawData:licenseNumber::VARCHAR AS licenseNumber,
    RawData:supervisorID::INTEGER AS supervisorID,
    RawData:roamingRep::BOOLEAN AS roamingRep,
    
    -- Skills & Teams
    RawData:skillIDs::ARRAY AS skillIDs,
    RawData:skillDescriptions::ARRAY AS skillDescriptions,
    RawData:linkedEmployeeIDs::ARRAY AS linkedEmployeeIDs,
    RawData:employeeLink::VARCHAR AS employeeLink,
    RawData:teamIDs::ARRAY AS teamIDs,
    RawData:primaryTeam::INTEGER AS primaryTeam,
    
    -- Access & Management
    RawData:accessControlProfileID::INTEGER AS accessControlProfileID,
    RawData:regionalManagerOfficeIDs::ARRAY AS regionalManagerOfficeIDs,
    
    -- Start Location
    RawData:startAddress::VARCHAR AS startAddress,
    RawData:startCity::VARCHAR AS startCity,
    RawData:startState::VARCHAR AS startState,
    RawData:startZip::VARCHAR AS startZip,
    RawData:startLat::FLOAT AS startLatitude,
    RawData:startLng::FLOAT AS startLongitude,
    
    -- End Location
    RawData:endAddress::VARCHAR AS endAddress,
    RawData:endCity::VARCHAR AS endCity,
    RawData:endState::VARCHAR AS endState,
    RawData:endZip::VARCHAR AS endZip,
    RawData:endLat::FLOAT AS endLatitude,
    RawData:endLng::FLOAT AS endLongitude,
    
    -- Dates
    RawData:lastLogin::TIMESTAMP_NTZ AS lastLogin,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
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
    RawData:appointmentID::INTEGER AS appointmentID,
    RawData:officeID::INTEGER AS officeID,
    RawData:customerID::INTEGER AS customerID,
    RawData:subscriptionID::INTEGER AS subscriptionID,
    RawData:subscriptionRegionID::INTEGER AS subscriptionRegionID,
    RawData:routeID::INTEGER AS routeID,
    RawData:spotID::INTEGER AS spotID,
    RawData:ticketID::INTEGER AS ticketID,
    
    -- Scheduling Information
    RawData:date::DATE AS appointmentDate,
    RawData:start::TIME AS startTime,
    RawData:end::TIME AS endTime,
    RawData:timeWindow::VARCHAR AS timeWindow,
    RawData:duration::INTEGER AS duration,
    RawData:callAhead::INTEGER AS callAhead,
    
    -- Type & Status
    RawData:type::INTEGER AS appointmentType,
    RawData:status::INTEGER AS status,
    RawData:statusText::VARCHAR AS statusText,
    RawData:isInitial::BOOLEAN AS isInitial,
    
    -- Employee Information
    RawData:employeeID::INTEGER AS employeeID,
    RawData:subscriptionPreferredTech::INTEGER AS subscriptionPreferredTech,
    RawData:completedBy::INTEGER AS completedBy,
    RawData:servicedBy::INTEGER AS servicedBy,
    RawData:additionalTechs::ARRAY AS additionalTechs,
    
    -- Completion Details
    RawData:dateCompleted::TIMESTAMP_NTZ AS dateCompleted,
    RawData:timeIn::TIME AS timeIn,
    RawData:timeOut::TIME AS timeOut,
    RawData:checkIn::TIMESTAMP_NTZ AS checkIn,
    RawData:checkOut::TIMESTAMP_NTZ AS checkOut,
    
    -- Service Details
    RawData:servicedInterior::BOOLEAN AS isInterior,
    RawData:windSpeed::INTEGER AS windSpeed,
    RawData:windDirection::VARCHAR AS windDirection,
    RawData:temperature::INTEGER AS temperature,
    
    -- Notes
    RawData:notes::VARCHAR AS appointmentNotes,
    RawData:officeNotes::VARCHAR AS officeNotes,
    
    -- Cancellation/Rescheduling
    RawData:dateCancelled::TIMESTAMP_NTZ AS dateCancelled,
    RawData:appointmentCancellationReason::VARCHAR AS appointmentCancelReason,
    RawData:cancellationReason::VARCHAR AS cancellationReason,
    RawData:cancellationReasonID::INTEGER AS cancellationReasonID,
    RawData:rescheduleReasonID::INTEGER AS rescheduleReasonID,
    RawData:reserviceReasonID::INTEGER AS reserviceReasonID,
    
    -- Target Pests (as array)
    RawData:targetPests::ARRAY AS targetPests,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS dateAdded,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
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
    RawData:ticketID::INTEGER AS ticketID,
    RawData:customerID::INTEGER AS customerID,
    RawData:billToAccountID::INTEGER AS billToAccountID,
    RawData:officeID::INTEGER AS officeID,
    RawData:appointmentID::INTEGER AS appointmentID,
    RawData:subscriptionID::INTEGER AS subscriptionID,
    RawData:serviceID::INTEGER AS serviceID,
    
    -- Ticket Information
    RawData:active::BOOLEAN AS isActive,
    RawData:invoiceDate::DATE AS invoiceDate,
    RawData:createdBy::INTEGER AS createdByEmployeeID,
    
    -- Financial Details
    RawData:subtotal::FLOAT AS subtotal,
    RawData:taxAmount::FLOAT AS taxAmount,
    RawData:total::FLOAT AS total,
    RawData:balance::FLOAT AS balance,
    RawData:serviceCharge::FLOAT AS serviceCharge,
    RawData:serviceTaxable::BOOLEAN AS serviceTaxable,
    RawData:productionValue::FLOAT AS productionValue,
    RawData:taxRate::FLOAT AS taxRate,
    
    -- Line Items (as JSON)
    RawData:items::ARRAY AS items,
    
    -- Dates
    RawData:dateCreated::TIMESTAMP_NTZ AS dateCreated,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
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
    RawData:paymentID::INTEGER AS paymentID,
    RawData:officeID::INTEGER AS officeID,
    RawData:customerID::INTEGER AS customerID,
    RawData:subscriptionID::INTEGER AS subscriptionID,
    RawData:originalPaymentID::INTEGER AS originalPaymentID,
    
    -- Payment Details
    RawData:date::DATE AS paymentDate,
    RawData:paymentMethod::INTEGER AS paymentMethod,
    RawData:amount::FLOAT AS amount,
    RawData:appliedAmount::FLOAT AS appliedAmount,
    RawData:unassignedAmount::FLOAT AS unassignedAmount,
    RawData:status::INTEGER AS status,
    
    -- Payment Type Flags
    RawData:officePayment::BOOLEAN AS isOfficePayment,
    RawData:collectionPayment::BOOLEAN AS isCollectionPayment,
    RawData:writeoff::BOOLEAN AS writeoff,
    RawData:creditMemo::BOOLEAN AS creditMemo,
    
    -- Processing Information
    RawData:paymentOrigin::VARCHAR AS paymentOrigin,
    RawData:paymentSource::VARCHAR AS paymentSource,
    RawData:transactionID::VARCHAR AS transactionID,
    
    -- Card Information
    RawData:lastFour::VARCHAR AS lastFour,
    RawData:cardType::VARCHAR AS cardType,
    
    -- References
    RawData:invoiceIDs::ARRAY AS invoiceIDs,
    RawData:employeeID::INTEGER AS employeeID,
    
    -- Batch Information
    RawData:batchOpened::TIMESTAMP_NTZ AS batchOpened,
    RawData:batchClosed::TIMESTAMP_NTZ AS batchClosed,
    
    -- Notes
    RawData:notes::VARCHAR AS paymentNotes,
    
    -- Dates
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- ROUTE VIEW - Daily route information
-- =====================================================
CREATE OR REPLACE VIEW VW_ROUTE AS
WITH latest_records AS (
    -- RouteID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:routeID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.ROUTE_FACT
)
SELECT 
    -- Keys
    RawData:routeID::INTEGER AS routeID,
    RawData:officeID::INTEGER AS officeID,
    RawData:templateID::INTEGER AS routeTemplateID,
    RawData:groupID::INTEGER AS routeGroupID,
    RawData:dayID::INTEGER AS dayID,
    
    -- Route Information
    RawData:title::VARCHAR AS routeTitle,
    RawData:groupTitle::VARCHAR AS routeGroupTitle,
    RawData:date::DATE AS routeDate,
    RawData:dayNotes::VARCHAR AS dayNotes,
    RawData:dayAlert::VARCHAR AS dayAlert,
    
    -- Employee Information
    RawData:addedBy::INTEGER AS addedByEmployeeID,
    RawData:assignedTech::INTEGER AS assignedTech,
    RawData:additionalTechs::ARRAY AS additionalTechs,
    
    -- Scheduling
    RawData:apiCanSchedule::BOOLEAN AS apiCanSchedule,
    RawData:scheduleTeams::ARRAY AS scheduleTeams,
    RawData:scheduleTypes::ARRAY AS scheduleTypes,
    RawData:lockedRoute::BOOLEAN AS isLocked,
    
    -- Route Metrics
    RawData:averageLatitude::FLOAT AS avgLatitude,
    RawData:averageLongitude::FLOAT AS avgLongitude,
    RawData:averageDistance::FLOAT AS avgDistance,
    RawData:totalDistance::FLOAT AS totalDistance,
    RawData:distanceScore::FLOAT AS distanceScore,
    RawData:estimatedAppointmentsDurationMinutes::INTEGER AS appointmentDurationEstimate,
    RawData:estimatedDrivingDurationSeconds::INTEGER AS drivingDurationEstimate,
    RawData:capacityEstimateValue::FLOAT AS capacityValueEstimate,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS dateAdded,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- TASK VIEW - Task management
-- =====================================================
CREATE OR REPLACE VIEW VW_TASK AS
WITH latest_records AS (
    -- TaskID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:taskID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.TASK_FACT
)
SELECT 
    -- Keys
    RawData:taskID::INTEGER AS taskID,
    RawData:officeID::INTEGER AS officeID,
    RawData:customerID::INTEGER AS customerID,
    RawData:referenceID::INTEGER AS referenceID,
    
    -- Task Information
    RawData:type::INTEGER AS type,
    RawData:category::INTEGER AS categoryID,
    RawData:categoryDescription::VARCHAR AS categoryDescription,
    RawData:task::VARCHAR AS description,
    RawData:status::INTEGER AS status,
    
    -- Employee References
    RawData:addedBy::INTEGER AS addedByEmployeeID,
    RawData:assignedTo::INTEGER AS assignedToEmployeeID,
    RawData:completedBy::INTEGER AS completedByEmployeeID,
    
    -- Task Details
    RawData:dueDate::DATE AS dueDate,
    RawData:completionNotes::VARCHAR AS completionNotes,
    RawData:phone::VARCHAR AS phone,
    
    -- Dates
    RawData:dateAdded::TIMESTAMP_NTZ AS dateAdded,
    RawData:dateCompleted::TIMESTAMP_NTZ AS dateCompleted,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
FROM latest_records
WHERE rn = 1;

-- =====================================================
-- NOTE VIEW - Customer notes
-- =====================================================
CREATE OR REPLACE VIEW VW_NOTE AS
WITH latest_records AS (
    -- NoteID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:noteID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.NOTE_FACT
)
SELECT 
    -- Keys
    RawData:noteID::INTEGER AS noteID,
    RawData:officeID::INTEGER AS officeID,
    RawData:customerID::INTEGER AS customerID,
    RawData:employeeID::INTEGER AS employeeID,
    RawData:referenceID::INTEGER AS referenceID,
    
    -- Note Information
    RawData:notes::VARCHAR AS text,
    RawData:typeID::INTEGER AS typeID,
    RawData:type::VARCHAR AS typeDescription,
    RawData:contactTypeCategories::ARRAY AS contactTypeCategories,
    
    -- Customer Information (denormalized)
    RawData:customerName::VARCHAR AS customerName,
    RawData:customerSpouse::VARCHAR AS customerSpouse,
    RawData:companyName::VARCHAR AS companyName,
    RawData:employeeName::VARCHAR AS employeeName,
    
    -- Visibility
    RawData:showCustomer::BOOLEAN AS isVisibleCustomer,
    RawData:showTech::BOOLEAN AS isVisibleTechnician,
    
    -- Cancellation Info
    RawData:cancellationReasonID::INTEGER AS cancellationReasonID,
    RawData:cancellationReason::VARCHAR AS cancellationReason,
    
    -- Email Tracking
    RawData:openCount::INTEGER AS openCount,
    RawData:clicksCount::INTEGER AS clickCount,
    RawData:emailStatus::VARCHAR AS emailStatus,
    
    -- Dates
    RawData:date::TIMESTAMP_NTZ AS dateCreated,
    RawData:dateAdded::TIMESTAMP_NTZ AS dateAdded,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated,
    
    -- Metadata
    LoadDatetimeUTC AS lastLoadTime
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
        ROW_NUMBER() OVER (PARTITION BY RawData:officeID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.OFFICE_DIM
)
SELECT 
    RawData:officeID::INTEGER AS officeID,
    RawData:officeName::VARCHAR AS officeName,
    RawData:companyID::INTEGER AS companyID,
    RawData:licenseNumber::VARCHAR AS licenseNumber,
    RawData:contactNumber::VARCHAR AS contactNumber,
    RawData:contactEmail::VARCHAR AS contactEmail,
    RawData:timezone::VARCHAR AS timezone,
    RawData:address::VARCHAR AS address,
    RawData:city::VARCHAR AS city,
    RawData:state::VARCHAR AS state,
    RawData:zip::VARCHAR AS zip,
    RawData:cautionStatements::VARCHAR AS cautionStatements,
    RawData:officeLatitude::FLOAT AS officeLatitude,
    RawData:officeLongitude::FLOAT AS officeLongitude
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
    RawData:regionID::INTEGER AS regionID,
    RawData:officeID::INTEGER AS officeID,
    RawData:type::VARCHAR AS regionType,
    RawData:active::BOOLEAN AS isActive,
    RawData:created::TIMESTAMP_NTZ AS dateCreated,
    RawData:deleted::TIMESTAMP_NTZ AS dateDeleted,
    RawData:points::VARIANT AS LatAndLong
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
    RawData:typeID::INTEGER AS serviceTypeID,
    RawData:officeID::INTEGER AS officeID,
    RawData:description::VARCHAR AS description,
    RawData:frequency::INTEGER AS frequency,
    RawData:defaultCharge::FLOAT AS defaultCharge,
    RawData:category::VARCHAR AS category,
    RawData:reservice::BOOLEAN AS isReserviceType,
    RawData:defaultLength::INTEGER AS defaultAppointmentLength,
    RawData:defaultInitialCharge::FLOAT AS defaultInitialCharge,
    RawData:initialID::INTEGER AS initialID,
    RawData:minimumRecurringCharge::FLOAT AS minRecurringCharge,
    RawData:minimumInitialCharge::FLOAT AS minInitialCharge,
    RawData:regularService::BOOLEAN AS isRegularService,
    RawData:initial::BOOLEAN AS isInitialService,
    RawData:seasonStart::DATE AS seasonStart,
    RawData:seasonEnd::DATE AS seasonEnd,
    RawData:sentricon::VARCHAR AS sentriconServiceType,
    RawData:visible::BOOLEAN AS isVisible,
    RawData:defaultFollowupDelay::INTEGER AS defaultFollowupDelay,
    RawData:salesVisible::BOOLEAN AS salesVisible
FROM latest_records
WHERE rn = 1;

-- Customer Source Dimension
CREATE OR REPLACE VIEW VW_DIM_CUSTOMER_SOURCE AS
WITH latest_records AS (
    -- SourceID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:sourceID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.CUSTOMERSOURCE_DIM
)
SELECT 
    RawData:sourceID::INTEGER AS sourceID,
    RawData:officeID::INTEGER AS officeID,
    RawData:source::VARCHAR AS sourceName,
    RawData:salesroutesDefault::BOOLEAN AS isSalesroutesDefault,
    RawData:visible::BOOLEAN AS isVisible,
    RawData:dealsSource::BOOLEAN AS isDealsSource
FROM latest_records
WHERE rn = 1;

-- Product Dimension
CREATE OR REPLACE VIEW VW_DIM_PRODUCT AS
WITH latest_records AS (
    -- ProductID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:productID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.PRODUCT_DIM
)
SELECT 
    RawData:productID::INTEGER AS productID,
    RawData:officeID::INTEGER AS officeID,
    RawData:description::VARCHAR AS description,
    RawData:glAccountID::INTEGER AS glAccountID,
    RawData:amount::FLOAT AS amount,
    RawData:taxable::BOOLEAN AS taxable,
    RawData:code::VARCHAR AS productCode,
    RawData:category::VARCHAR AS productCategory,
    RawData:visible::BOOLEAN AS isVisible,
    RawData:salesVisible::BOOLEAN AS isSalesVisible,
    RawData:recurring::BOOLEAN AS isRecurring
FROM latest_records
WHERE rn = 1;

-- Generic Flag Dimension
CREATE OR REPLACE VIEW VW_DIM_GENERIC_FLAG AS
WITH latest_records AS (
    -- GenericFlagID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:genericFlagID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.GENERICFLAG_DIM
)
SELECT 
    RawData:genericFlagID::INTEGER AS genericFlagID,
    RawData:officeID::INTEGER AS officeID,
    RawData:code::VARCHAR AS flagCode,
    RawData:description::VARCHAR AS flagDescription,
    RawData:status::INTEGER AS flagStatus,
    RawData:type::VARCHAR AS flagType,
    RawData:dateCreated::TIMESTAMP_NTZ AS dateCreated,
    RawData:dateUpdated::TIMESTAMP_NTZ AS dateUpdated
FROM latest_records
WHERE rn = 1;

-- Reservice Reason Dimension
CREATE OR REPLACE VIEW VW_DIM_RESERVICE_REASON AS
WITH latest_records AS (
    -- ReserviceReasonID is unique across ALL offices
    SELECT 
        OfficeID,
        RawData,
        LoadDatetimeUTC,
        ROW_NUMBER() OVER (PARTITION BY RawData:reserviceReasonID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
    FROM RAW.FIELDROUTES.RESERVICEREASON_DIM
)
SELECT 
    RawData:reserviceReasonID::INTEGER AS reserviceReasonID,
    RawData:officeID::INTEGER AS officeID,
    RawData:visible::BOOLEAN AS isVisible,
    RawData:reason::VARCHAR AS description
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
    c.fName,
    c.lName,
    c.companyName,
    c.regionID,
    DATEDIFF(day, c.dateAdded, ccr.DateCancelled) AS customerLifetimeDays
FROM RAW.FIELDROUTES.CUSTOMERCANCELLATIONREASONS_FACT ccr
LEFT JOIN VW_CUSTOMER c 
    ON ccr.CustomerID = c.customerID;

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