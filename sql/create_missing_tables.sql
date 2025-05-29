-- Create missing tables in Snowflake for FieldRoutes ETL
-- Run this in the RAW.FIELDROUTES schema

USE DATABASE RAW;
USE SCHEMA FIELDROUTES;

-- Dimension Tables
CREATE TABLE IF NOT EXISTS OFFICE_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS REGION_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS SERVICETYPE_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS CUSTOMERSOURCE_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS GENERICFLAG_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS CANCELLATIONREASON_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS CUSTOMERFLAG_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS PRODUCT_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS RESERVICEREASON_DIM (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

-- Fact Tables
CREATE TABLE IF NOT EXISTS CUSTOMER_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS EMPLOYEE_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS APPOINTMENT_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS SUBSCRIPTION_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS ROUTE_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS TICKET_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS TICKETITEM_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS PAYMENT_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS APPLIEDPAYMENT_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS NOTE_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS TASK_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS DOORKNOCK_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

-- Financial Transaction table (for both disbursements and chargebacks)
CREATE TABLE IF NOT EXISTS FINANCIALTRANSACTION_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    TransactionType VARCHAR(50),
    PRIMARY KEY (OfficeID, LoadDatetimeUTC, TransactionType)
);

CREATE TABLE IF NOT EXISTS ADDITIONALCONTACTS_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS DISBURSEMENTITEM_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS GENERICFLAGASSIGNMENT_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS KNOCK_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS PAYMENTPROFILE_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

-- Legacy tables
CREATE TABLE IF NOT EXISTS APPOINTMENTREMINDER_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

CREATE TABLE IF NOT EXISTS FLAGASSIGNMENT_FACT (
    OfficeID INTEGER NOT NULL,
    LoadDatetimeUTC TIMESTAMP_NTZ NOT NULL,
    RawData VARIANT,
    PRIMARY KEY (OfficeID, LoadDatetimeUTC)
);

-- Aggregation table
CREATE TABLE IF NOT EXISTS CUSTOMERCANCELLATIONREASONS_FACT (
    OfficeID INTEGER,
    CustomerID INTEGER,
    CancellationReasonID INTEGER,
    CancellationReasonDescription VARCHAR,
    DateCancelled TIMESTAMP_NTZ,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    PRIMARY KEY (OfficeID, CustomerID, CancellationReasonID)
);

-- Create staging tables for each entity (used during load process)
-- These are temporary tables used to stage data before moving to final tables
CREATE TABLE IF NOT EXISTS OFFICE_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS REGION_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS SERVICETYPE_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS CUSTOMERSOURCE_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS GENERICFLAG_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS CANCELLATIONREASON_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS CUSTOMERFLAG_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS PRODUCT_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS RESERVICEREASON_DIM_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS CUSTOMER_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS EMPLOYEE_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS APPOINTMENT_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS SUBSCRIPTION_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS ROUTE_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS TICKET_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS TICKETITEM_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS PAYMENT_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS APPLIEDPAYMENT_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS NOTE_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS TASK_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS DOORKNOCK_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS FINANCIALTRANSACTION_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR,
    TransactionType VARCHAR
);

CREATE TABLE IF NOT EXISTS ADDITIONALCONTACTS_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS DISBURSEMENTITEM_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS GENERICFLAGASSIGNMENT_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS KNOCK_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS PAYMENTPROFILE_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS APPOINTMENTREMINDER_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

CREATE TABLE IF NOT EXISTS FLAGASSIGNMENT_FACT_staging (
    OfficeID INTEGER,
    LoadDatetimeUTC TIMESTAMP_NTZ,
    RawDataString VARCHAR
);

-- Grant permissions if needed (adjust role names as necessary)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FIELDROUTES TO ROLE YOUR_ETL_ROLE;

-- Verify tables were created
SHOW TABLES LIKE '%_DIM';
SHOW TABLES LIKE '%_FACT';