"""
Transform Raw Data to Analytics Tables
Processes data from RAW schema to populate ANALYTICS schema tables
"""
from datetime import datetime, timezone
from typing import List, Dict, Optional
from prefect import flow, task, get_run_logger
from prefect_snowflake import SnowflakeConnector


@task(name="create_staging_schema")
def create_staging_schema(snowflake: SnowflakeConnector) -> None:
    """Create STAGING_DB and schema if they don't exist"""
    logger = get_run_logger()
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS STAGING_DB")
            cursor.execute("USE DATABASE STAGING_DB")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS FIELDROUTES")
            logger.info("STAGING_DB.FIELDROUTES schema ready")


@task(name="transform_dimension_tables")
def transform_dimension_tables(snowflake: SnowflakeConnector) -> None:
    """Transform dimension tables from raw to analytics schema"""
    logger = get_run_logger()
    
    dimension_transformations = {
        "DIM_OFFICE": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_OFFICE AS
            SELECT DISTINCT
                RawData:officeID::INTEGER as OfficeID,
                RawData:officeName::STRING as OfficeName,
                RawData:regionID::INTEGER as RegionID,
                RawData:address::STRING as Address,
                RawData:city::STRING as City,
                RawData:state::STRING as State,
                RawData:zip::STRING as ZipCode,
                RawData:phone::STRING as Phone,
                RawData:isActive::BOOLEAN as IsActive,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.OFFICE_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.OFFICE_DIM
            )
        """,
        
        "DIM_REGION": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_REGION AS
            SELECT DISTINCT
                RawData:regionID::INTEGER as RegionID,
                RawData:regionName::STRING as RegionName,
                RawData:isActive::BOOLEAN as IsActive,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.REGION_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.REGION_DIM
            )
        """,
        
        "DIM_SERVICE_TYPE": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_SERVICE_TYPE AS
            SELECT DISTINCT
                RawData:typeID::INTEGER as ServiceTypeID,
                RawData:description::STRING as ServiceTypeName,
                RawData:category::STRING as Category,
                RawData:isRecurring::BOOLEAN as IsRecurring,
                RawData:defaultCharge::FLOAT as DefaultCharge,
                RawData:isActive::BOOLEAN as IsActive,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.SERVICETYPE_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.SERVICETYPE_DIM
            )
        """,
        
        "DIM_CUSTOMER_SOURCE": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_CUSTOMER_SOURCE AS
            SELECT DISTINCT
                RawData:sourceID::INTEGER as SourceID,
                RawData:source::STRING as SourceName,
                RawData:isActive::BOOLEAN as IsActive,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.CUSTOMERSOURCE_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.CUSTOMERSOURCE_DIM
            )
        """,
        
        "DIM_PRODUCT": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_PRODUCT AS
            SELECT DISTINCT
                RawData:productID::INTEGER as ProductID,
                RawData:productName::STRING as ProductName,
                RawData:category::STRING as Category,
                RawData:unitCost::FLOAT as UnitCost,
                RawData:isActive::BOOLEAN as IsActive,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.PRODUCT_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.PRODUCT_DIM
            )
        """,
        
        "DIM_CANCELLATION_REASON": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_CANCELLATION_REASON AS
            SELECT DISTINCT
                RawData:reasonID::INTEGER as ReasonID,
                RawData:reason::STRING as ReasonName,
                RawData:isActive::BOOLEAN as IsActive,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.CANCELLATIONREASON_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.CANCELLATIONREASON_DIM
            )
        """
    }
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            for table_name, sql in dimension_transformations.items():
                logger.info(f"Transforming {table_name}")
                cursor.execute(sql)
                row_count = cursor.fetchone()[0] if cursor.rowcount == -1 else cursor.rowcount
                logger.info(f"Created {table_name} with {row_count} rows")


@task(name="transform_fact_tables")
def transform_fact_tables(snowflake: SnowflakeConnector, incremental: bool = True) -> None:
    """Transform fact tables from raw to analytics schema"""
    logger = get_run_logger()
    
    # For incremental loads, only process records from last 48 hours
    # For full loads, process all records
    where_clause = """
        WHERE LoadDatetimeUTC >= DATEADD(hour, -48, CURRENT_TIMESTAMP())
    """ if incremental else ""
    
    fact_transformations = {
        "FACT_CUSTOMER": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_CUSTOMER tgt
            USING (
                SELECT 
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:fname::STRING as FirstName,
                    RawData:lname::STRING as LastName,
                    RawData:companyName::STRING as CompanyName,
                    RawData:email::STRING as Email,
                    RawData:phone1::STRING as Phone1,
                    RawData:phone2::STRING as Phone2,
                    RawData:address::STRING as Address,
                    RawData:city::STRING as City,
                    RawData:state::STRING as State,
                    RawData:zip::STRING as ZipCode,
                    RawData:billingCompanyName::STRING as BillingCompanyName,
                    RawData:billingFName::STRING as BillingFirstName,
                    RawData:billingLName::STRING as BillingLastName,
                    RawData:billingAddress::STRING as BillingAddress,
                    RawData:billingCity::STRING as BillingCity,
                    RawData:billingState::STRING as BillingState,
                    RawData:billingZip::STRING as BillingZip,
                    RawData:balance::FLOAT as Balance,
                    RawData:balanceAge::INTEGER as BalanceAge,
                    RawData:sourceID::INTEGER as SourceID,
                    RawData:status::STRING as Status,
                    RawData:dateAdded::TIMESTAMP_NTZ as DateAdded,
                    RawData:dateUpdated::TIMESTAMP_NTZ as DateUpdated,
                    RawData:dateCancelled::TIMESTAMP_NTZ as DateCancelled,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:customerID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.CUSTOMER_FACT
                {where_clause}
            ) src
            ON tgt.CustomerID = src.CustomerID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                FirstName = src.FirstName,
                LastName = src.LastName,
                CompanyName = src.CompanyName,
                Email = src.Email,
                Phone1 = src.Phone1,
                Phone2 = src.Phone2,
                Address = src.Address,
                City = src.City,
                State = src.State,
                ZipCode = src.ZipCode,
                BillingCompanyName = src.BillingCompanyName,
                BillingFirstName = src.BillingFirstName,
                BillingLastName = src.BillingLastName,
                BillingAddress = src.BillingAddress,
                BillingCity = src.BillingCity,
                BillingState = src.BillingState,
                BillingZip = src.BillingZip,
                Balance = src.Balance,
                BalanceAge = src.BalanceAge,
                SourceID = src.SourceID,
                Status = src.Status,
                DateAdded = src.DateAdded,
                DateUpdated = src.DateUpdated,
                DateCancelled = src.DateCancelled,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                CustomerID, OfficeID, FirstName, LastName, CompanyName, Email,
                Phone1, Phone2, Address, City, State, ZipCode,
                BillingCompanyName, BillingFirstName, BillingLastName,
                BillingAddress, BillingCity, BillingState, BillingZip,
                Balance, BalanceAge, SourceID, Status,
                DateAdded, DateUpdated, DateCancelled, LoadDatetimeUTC
            ) VALUES (
                src.CustomerID, src.OfficeID, src.FirstName, src.LastName, src.CompanyName, src.Email,
                src.Phone1, src.Phone2, src.Address, src.City, src.State, src.ZipCode,
                src.BillingCompanyName, src.BillingFirstName, src.BillingLastName,
                src.BillingAddress, src.BillingCity, src.BillingState, src.BillingZip,
                src.Balance, src.BalanceAge, src.SourceID, src.Status,
                src.DateAdded, src.DateUpdated, src.DateCancelled, src.LoadDatetimeUTC
            )
        """,
        
        "FACT_TICKET": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_TICKET tgt
            USING (
                SELECT 
                    RawData:ticketID::INTEGER as TicketID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:subscriptionID::INTEGER as SubscriptionID,
                    RawData:appointmentID::INTEGER as AppointmentID,
                    RawData:serviceTypeID::INTEGER as ServiceTypeID,
                    RawData:invoiceNumber::STRING as InvoiceNumber,
                    RawData:total::FLOAT as TotalAmount,
                    RawData:subtotal::FLOAT as SubtotalAmount,
                    RawData:taxAmount::FLOAT as TaxAmount,
                    RawData:balance::FLOAT as Balance,
                    RawData:serviceCharge::FLOAT as ServiceCharge,
                    RawData:date::DATE as ServiceDate,
                    RawData:dateCreated::TIMESTAMP_NTZ as DateCreated,
                    RawData:dateUpdated::TIMESTAMP_NTZ as DateUpdated,
                    RawData:completedOn::TIMESTAMP_NTZ as CompletedOn,
                    RawData:status::STRING as Status,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:ticketID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.TICKET_FACT
                {where_clause}
            ) src
            ON tgt.TicketID = src.TicketID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                CustomerID = src.CustomerID,
                SubscriptionID = src.SubscriptionID,
                AppointmentID = src.AppointmentID,
                ServiceTypeID = src.ServiceTypeID,
                InvoiceNumber = src.InvoiceNumber,
                TotalAmount = src.TotalAmount,
                SubtotalAmount = src.SubtotalAmount,
                TaxAmount = src.TaxAmount,
                Balance = src.Balance,
                ServiceCharge = src.ServiceCharge,
                ServiceDate = src.ServiceDate,
                DateCreated = src.DateCreated,
                DateUpdated = src.DateUpdated,
                CompletedOn = src.CompletedOn,
                Status = src.Status,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                TicketID, OfficeID, CustomerID, SubscriptionID, AppointmentID,
                ServiceTypeID, InvoiceNumber, TotalAmount, SubtotalAmount, TaxAmount,
                Balance, ServiceCharge, ServiceDate, DateCreated, DateUpdated,
                CompletedOn, Status, LoadDatetimeUTC
            ) VALUES (
                src.TicketID, src.OfficeID, src.CustomerID, src.SubscriptionID, src.AppointmentID,
                src.ServiceTypeID, src.InvoiceNumber, src.TotalAmount, src.SubtotalAmount, src.TaxAmount,
                src.Balance, src.ServiceCharge, src.ServiceDate, src.DateCreated, src.DateUpdated,
                src.CompletedOn, src.Status, src.LoadDatetimeUTC
            )
        """,
        
        "FACT_PAYMENT": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_PAYMENT tgt
            USING (
                SELECT 
                    RawData:paymentID::INTEGER as PaymentID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:amount::FLOAT as Amount,
                    RawData:appliedAmount::FLOAT as AppliedAmount,
                    RawData:paymentMethod::STRING as PaymentMethod,
                    RawData:checkNumber::STRING as CheckNumber,
                    RawData:date::DATE as PaymentDate,
                    RawData:appliedOn::TIMESTAMP_NTZ as AppliedOn,
                    RawData:dateCreated::TIMESTAMP_NTZ as DateCreated,
                    RawData:dateUpdated::TIMESTAMP_NTZ as DateUpdated,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:paymentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.PAYMENT_FACT
                {where_clause}
            ) src
            ON tgt.PaymentID = src.PaymentID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                CustomerID = src.CustomerID,
                Amount = src.Amount,
                AppliedAmount = src.AppliedAmount,
                PaymentMethod = src.PaymentMethod,
                CheckNumber = src.CheckNumber,
                PaymentDate = src.PaymentDate,
                AppliedOn = src.AppliedOn,
                DateCreated = src.DateCreated,
                DateUpdated = src.DateUpdated,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                PaymentID, OfficeID, CustomerID, Amount, AppliedAmount,
                PaymentMethod, CheckNumber, PaymentDate, AppliedOn,
                DateCreated, DateUpdated, LoadDatetimeUTC
            ) VALUES (
                src.PaymentID, src.OfficeID, src.CustomerID, src.Amount, src.AppliedAmount,
                src.PaymentMethod, src.CheckNumber, src.PaymentDate, src.AppliedOn,
                src.DateCreated, src.DateUpdated, src.LoadDatetimeUTC
            )
        """,
        
        "FACT_APPLIED_PAYMENT": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT tgt
            USING (
                SELECT 
                    RawData:appliedPaymentID::INTEGER as AppliedPaymentID,
                    RawData:paymentID::INTEGER as PaymentID,
                    RawData:ticketID::INTEGER as TicketID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:appliedAmount::FLOAT as AppliedAmount,
                    RawData:appliedOn::TIMESTAMP_NTZ as AppliedDate,
                    RawData:dateUpdated::TIMESTAMP_NTZ as DateUpdated,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:appliedPaymentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.APPLIEDPAYMENT_FACT
                {where_clause}
            ) src
            ON tgt.AppliedPaymentID = src.AppliedPaymentID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                PaymentID = src.PaymentID,
                TicketID = src.TicketID,
                OfficeID = src.OfficeID,
                AppliedAmount = src.AppliedAmount,
                AppliedDate = src.AppliedDate,
                DateUpdated = src.DateUpdated,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                AppliedPaymentID, PaymentID, TicketID, OfficeID,
                AppliedAmount, AppliedDate, DateUpdated, LoadDatetimeUTC
            ) VALUES (
                src.AppliedPaymentID, src.PaymentID, src.TicketID, src.OfficeID,
                src.AppliedAmount, src.AppliedDate, src.DateUpdated, src.LoadDatetimeUTC
            )
        """
    }
    
    # First create tables if they don't exist
    create_statements = {
        "FACT_CUSTOMER": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_CUSTOMER (
                CustomerID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                FirstName STRING,
                LastName STRING,
                CompanyName STRING,
                Email STRING,
                Phone1 STRING,
                Phone2 STRING,
                Address STRING,
                City STRING,
                State STRING,
                ZipCode STRING,
                BillingCompanyName STRING,
                BillingFirstName STRING,
                BillingLastName STRING,
                BillingAddress STRING,
                BillingCity STRING,
                BillingState STRING,
                BillingZip STRING,
                Balance FLOAT,
                BalanceAge INTEGER,
                SourceID INTEGER,
                Status STRING,
                DateAdded TIMESTAMP_NTZ,
                DateUpdated TIMESTAMP_NTZ,
                DateCancelled TIMESTAMP_NTZ,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        "FACT_TICKET": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_TICKET (
                TicketID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                CustomerID INTEGER,
                SubscriptionID INTEGER,
                AppointmentID INTEGER,
                ServiceTypeID INTEGER,
                InvoiceNumber STRING,
                TotalAmount FLOAT,
                SubtotalAmount FLOAT,
                TaxAmount FLOAT,
                Balance FLOAT,
                ServiceCharge FLOAT,
                ServiceDate DATE,
                DateCreated TIMESTAMP_NTZ,
                DateUpdated TIMESTAMP_NTZ,
                CompletedOn TIMESTAMP_NTZ,
                Status STRING,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        "FACT_PAYMENT": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_PAYMENT (
                PaymentID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                CustomerID INTEGER,
                Amount FLOAT,
                AppliedAmount FLOAT,
                PaymentMethod STRING,
                CheckNumber STRING,
                PaymentDate DATE,
                AppliedOn TIMESTAMP_NTZ,
                DateCreated TIMESTAMP_NTZ,
                DateUpdated TIMESTAMP_NTZ,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        "FACT_APPLIED_PAYMENT": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT (
                AppliedPaymentID INTEGER PRIMARY KEY,
                PaymentID INTEGER,
                TicketID INTEGER,
                OfficeID INTEGER,
                AppliedAmount FLOAT,
                AppliedDate TIMESTAMP_NTZ,
                DateUpdated TIMESTAMP_NTZ,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """
    }
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            # Create tables first
            for table_name, create_sql in create_statements.items():
                cursor.execute(create_sql)
                logger.info(f"Ensured {table_name} exists")
            
            # Then run merges
            for table_name, merge_sql in fact_transformations.items():
                logger.info(f"Transforming {table_name}")
                cursor.execute(merge_sql)
                logger.info(f"Completed {table_name} transformation")


@task(name="refresh_reporting_views")
def refresh_reporting_views(snowflake: SnowflakeConnector) -> None:
    """Refresh materialized views if needed"""
    logger = get_run_logger()
    
    # List of views that might benefit from materialization
    critical_views = [
        "VW_AR_AGING",
        "VW_DSO_METRICS",
        "VW_CEI_METRICS",
        "VW_CUSTOMER",
        "VW_TICKET",
        "VW_PAYMENT",
        "VW_APPLIED_PAYMENT"
    ]
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            for view_name in critical_views:
                # Check if view exists
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.VIEWS 
                    WHERE TABLE_SCHEMA = 'PUBLIC' 
                    AND TABLE_NAME = '{view_name}'
                """)
                
                if cursor.fetchone()[0] > 0:
                    logger.info(f"View {view_name} exists and is ready for use")
                else:
                    logger.warning(f"View {view_name} not found - may need to run create_reporting_views_v2.sql")


@task(name="validate_data_quality")
def validate_data_quality(snowflake: SnowflakeConnector) -> Dict[str, any]:
    """Run data quality checks on transformed data"""
    logger = get_run_logger()
    
    quality_checks = {
        "customer_orphans": """
            SELECT COUNT(*) as orphan_count
            FROM STAGING_DB.FIELDROUTES.FACT_TICKET t
            LEFT JOIN STAGING_DB.FIELDROUTES.FACT_CUSTOMER c ON t.CustomerID = c.CustomerID
            WHERE c.CustomerID IS NULL
        """,
        
        "payment_without_ticket": """
            SELECT COUNT(*) as unlinked_payments
            FROM STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT ap
            LEFT JOIN STAGING_DB.FIELDROUTES.FACT_TICKET t ON ap.TicketID = t.TicketID
            WHERE t.TicketID IS NULL
        """,
        
        "negative_balances": """
            SELECT COUNT(*) as negative_balance_count
            FROM STAGING_DB.FIELDROUTES.FACT_CUSTOMER
            WHERE Balance < 0
        """,
        
        "future_dated_tickets": """
            SELECT COUNT(*) as future_tickets
            FROM STAGING_DB.FIELDROUTES.FACT_TICKET
            WHERE CompletedOn > CURRENT_TIMESTAMP()
        """
    }
    
    results = {}
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            for check_name, query in quality_checks.items():
                cursor.execute(query)
                result = cursor.fetchone()[0]
                results[check_name] = result
                
                if result > 0:
                    logger.warning(f"Data quality issue: {check_name} = {result}")
                else:
                    logger.info(f"Data quality check passed: {check_name}")
    
    return results


@flow(name="transform-raw-to-staging")
def transform_raw_to_staging(
    incremental: bool = True,
    run_quality_checks: bool = True
):
    """
    Transform raw FieldRoutes data to staging tables
    
    Args:
        incremental: If True, only process recent data. If False, full reload.
        run_quality_checks: If True, validate data quality after transformation.
    """
    logger = get_run_logger()
    logger.info(f"Starting transformation - Mode: {'Incremental' if incremental else 'Full'}")
    
    # Get Snowflake connection
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    
    # Create staging schema
    create_staging_schema(snowflake)
    
    # Transform dimension tables (always full refresh for dims)
    transform_dimension_tables(snowflake)
    
    # Transform fact tables
    transform_fact_tables(snowflake, incremental=incremental)
    
    # Refresh/validate reporting views
    refresh_reporting_views(snowflake)
    
    # Run data quality checks
    if run_quality_checks:
        quality_results = validate_data_quality(snowflake)
        logger.info(f"Data quality check results: {quality_results}")
    
    logger.info("Transformation completed successfully")


if __name__ == "__main__":
    # Run full transformation
    transform_raw_to_analytics(incremental=False, run_quality_checks=True)