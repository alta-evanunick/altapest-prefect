"""
Transform Raw Data to Staging Tables - Complete Version
Includes all columns from FR_SF_Lookup.csv
"""
from datetime import datetime, timezone
from typing import List, Dict, Optional
from prefect import flow, task, get_run_logger
from prefect_snowflake import SnowflakeConnector


@task(name="create_staging_schema")
def create_staging_schema() -> None:
    """Create STAGING_DB and schema if they don't exist"""
    logger = get_run_logger()
    
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE ALTAPESTANALYTICS")  # Add warehouse selection
            cursor.execute("CREATE DATABASE IF NOT EXISTS STAGING_DB")
            cursor.execute("USE DATABASE STAGING_DB")
            cursor.execute("CREATE SCHEMA IF NOT EXISTS FIELDROUTES")
            logger.info("STAGING_DB.FIELDROUTES schema ready")


@task(name="transform_dimension_tables")
def transform_dimension_tables() -> None:
    """Transform dimension tables from raw to staging schema"""
    logger = get_run_logger()
    
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    dimension_transformations = {
        "DIM_OFFICE": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_OFFICE AS
            SELECT DISTINCT
                RawData:officeID::INTEGER as OfficeID,
                RawData:officeName::STRING as OfficeName,
                RawData:companyID::INTEGER as CompanyID,
                RawData:licenseNumber::STRING as LicenseNumber,
                RawData:contactNumber::STRING as ContactNumber,
                RawData:contactEmail::STRING as ContactEmail,
                RawData:timezone::STRING as Timezone,
                RawData:address::STRING as Address,
                RawData:city::STRING as City,
                RawData:state::STRING as State,
                RawData:zip::STRING as ZipCode,
                RawData:cautionStatements::STRING as CautionStatements,
                RawData:officeLatitude::FLOAT as OfficeLatitude,
                RawData:officeLongitude::FLOAT as OfficeLongitude,
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
                RawData:officeID::INTEGER as OfficeID,
                CASE WHEN RawData:created::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:created IS NULL 
                     THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:created::STRING) END as DateCreated,
                CASE WHEN RawData:deleted::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:deleted IS NULL 
                     THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:deleted::STRING) END as DateDeleted,
                RawData:points::VARIANT as LatAndLong,
                RawData:type::STRING as RegionType,
                RawData:active::BOOLEAN as IsActive,
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
                RawData:officeID::INTEGER as OfficeID,
                RawData:description::STRING as Description,
                RawData:frequency::STRING as Frequency,
                RawData:defaultCharge::FLOAT as DefaultCharge,
                RawData:category::STRING as Category,
                RawData:reservice::INTEGER as IsReserviceType,
                RawData:defaultLength::INTEGER as DefaultAppointmentLength,
                RawData:defaultInitialCharge::FLOAT as DefaultInitialCharge,
                RawData:initialID::INTEGER as InitialID,
                RawData:minimumRecurringCharge::FLOAT as MinRecurringCharge,
                RawData:minimumInitialCharge::FLOAT as MinInitialCharge,
                RawData:regularService::INTEGER as IsRegularService,
                RawData:initial::INTEGER as IsInitialService,
                RawData:seasonStart::STRING as SeasonStart,
                RawData:seasonEnd::STRING as SeasonEnd,
                RawData:sentricon::STRING as SentriconServiceType,
                RawData:visible::BOOLEAN as IsVisible,
                RawData:defaultFollowupDelay::INTEGER as DefaultFollowupDelay,
                RawData:salesVisible::BOOLEAN as SalesVisible,
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
                RawData:officeID::INTEGER as OfficeID,
                RawData:source::STRING as SourceName,
                RawData:salesroutesDefault::INTEGER as IsSalesroutesDefault,
                RawData:visible::BOOLEAN as IsVisible,
                RawData:dealsSource::INTEGER as IsDealsSource,
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
                RawData:officeID::INTEGER as OfficeID,
                RawData:description::STRING as Description,
                RawData:glAccountID::INTEGER as GLAccountID,
                RawData:amount::FLOAT as Amount,
                RawData:taxable::INTEGER as Taxable,
                RawData:code::STRING as ProductCode,
                RawData:category::STRING as ProductCategory,
                RawData:visible::BOOLEAN as IsVisible,
                RawData:salesVisible::INTEGER as IsSalesVisible,
                RawData:recurring::INTEGER as IsRecurring,
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
        """,
        
        "DIM_GENERIC_FLAG": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_GENERIC_FLAG AS
            SELECT DISTINCT
                RawData:genericFlagID::INTEGER as GenericFlagID,
                RawData:officeID::INTEGER as OfficeID,
                RawData:code::STRING as FlagCode,
                RawData:description::STRING as FlagDescription,
                RawData:status::STRING as FlagStatus,
                RawData:type::STRING as FlagType,
                CASE WHEN RawData:dateCreated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateCreated IS NULL 
                     THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateCreated::STRING) END as DateCreated,
                CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                     THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.GENERICFLAG_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.GENERICFLAG_DIM
            )
        """,
        
        "DIM_RESERVICE_REASON": """
            CREATE OR REPLACE TABLE STAGING_DB.FIELDROUTES.DIM_RESERVICE_REASON AS
            SELECT DISTINCT
                RawData:reserviceReasonID::INTEGER as ReserviceReasonID,
                RawData:officeID::INTEGER as OfficeID,
                RawData:visible::BOOLEAN as IsVisible,
                RawData:reason::STRING as Description,
                CURRENT_TIMESTAMP() as LoadDatetimeUTC
            FROM RAW_DB.FIELDROUTES.RESERVICEREASON_DIM
            WHERE LoadDatetimeUTC = (
                SELECT MAX(LoadDatetimeUTC) FROM RAW_DB.FIELDROUTES.RESERVICEREASON_DIM
            )
        """
    }
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE ALTAPESTANALYTICS")  # Add warehouse selection
            for table_name, sql in dimension_transformations.items():
                logger.info(f"Transforming {table_name}")
                cursor.execute(sql)
                row_count = cursor.fetchone()[0] if cursor.rowcount == -1 else cursor.rowcount
                logger.info(f"Created {table_name} with {row_count} rows")


@task(name="transform_fact_tables")
def transform_fact_tables(incremental: bool = True) -> None:
    """Transform fact tables from raw to staging schema"""
    logger = get_run_logger()
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    
    # For incremental loads, only process records from last 48 hours
    where_clause = """
        WHERE LoadDatetimeUTC >= DATEADD(hour, -48, CURRENT_TIMESTAMP())
    """ if incremental else ""
    
    fact_transformations = {
        "FACT_CUSTOMER": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_CUSTOMER tgt
            USING (
                SELECT 
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:billToAccountID::INTEGER as BillToAccountID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:fname::STRING as FName,
                    RawData:lname::STRING as LName,
                    RawData:companyName::STRING as CompanyName,
                    RawData:spouse::STRING as Spouse,
                    RawData:commercialAccount::INTEGER as IsCommercial,
                    RawData:status::STRING as Status,
                    RawData:statusText::STRING as StatusText,
                    RawData:email::STRING as Email,
                    RawData:phone1::STRING as Phone1,
                    RawData:ext1::STRING as Phone1_Ext,
                    RawData:phone2::STRING as Phone2,
                    RawData:ext2::STRING as Phone2_Ext,
                    RawData:address::STRING as Address,
                    RawData:city::STRING as City,
                    RawData:state::STRING as State,
                    RawData:zip::STRING as Zip,
                    RawData:billingCompanyName::STRING as BillingCompanyName,
                    RawData:billingFName::STRING as BillingFName,
                    RawData:billingLName::STRING as BillingLName,
                    RawData:billingAddress::STRING as BillingAddress,
                    RawData:billingCity::STRING as BillingCity,
                    RawData:billingState::STRING as BillingState,
                    RawData:billingZip::STRING as BillingZip,
                    RawData:billingPhone::STRING as BillingPhone,
                    RawData:billingEmail::STRING as BillingEmail,
                    RawData:lat::FLOAT as Latitude,
                    RawData:lng::FLOAT as Longitude,
                    RawData:squareFeet::INTEGER as SqFeet,
                    RawData:addedByID::INTEGER as AddedByEmployeeID,
                    CASE WHEN RawData:dateAdded::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateAdded IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateAdded::STRING) END as DateAdded,
                    CASE WHEN RawData:dateCancelled::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateCancelled IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateCancelled::STRING) END as DateCancelled,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    RawData:sourceID::INTEGER as SourceID,
                    RawData:source::STRING as Source,
                    RawData:aPay::STRING as APay,
                    RawData:preferredTechID::INTEGER as PreferredTechID,
                    RawData:paidInFull::INTEGER as PaidInFull,
                    RawData:subscriptionIDs::VARIANT as SubscriptionIDs,
                    RawData:balance::FLOAT as Balance,
                    RawData:balanceAge::INTEGER as BalanceAge,
                    RawData:responsibleBalance::FLOAT as ResponsibleBalance,
                    RawData:responsibleBalanceAge::INTEGER as ResponsibleBalanceAge,
                    RawData:masterAccount::INTEGER as MasterAccount,
                    RawData:preferredBillingDate::STRING as PreferredBillingDate,
                    CASE WHEN RawData:paymentHoldDate::STRING IN ('0000-00-00', '', '0000-00-00 00:00:00') OR RawData:paymentHoldDate IS NULL 
                         THEN NULL ELSE TRY_TO_DATE(RawData:paymentHoldDate::STRING) END as PaymentHoldDate,
                    RawData:mostRecentCreditCardLastFour::STRING as LatestCCLastFour,
                    RawData:mostRecentCreditCardExpirationDate::STRING as LatestCCExpDate,
                    RawData:appointmentIDs::VARIANT as AppointmentIDs,
                    RawData:ticketIDs::VARIANT as TicketIDs,
                    RawData:paymentIDs::VARIANT as PaymentIDs,
                    RawData:regionID::INTEGER as RegionID,
                    RawData:specialScheduling::STRING as SpecialScheduling,
                    RawData:taxRate::FLOAT as TaxRate,
                    RawData:stateTax::FLOAT as StateTax,
                    RawData:cityTax::FLOAT as CityTax,
                    RawData:countyTax::FLOAT as CountyTax,
                    RawData:districtTax::FLOAT as DistrictTax,
                    RawData:districtTax1::FLOAT as DistrictTax1,
                    RawData:districtTax2::FLOAT as DistrictTax2,
                    RawData:districtTax3::FLOAT as DistrictTax3,
                    RawData:districtTax4::FLOAT as DistrictTax4,
                    RawData:districtTax5::FLOAT as DistrictTax5,
                    RawData:customTax::FLOAT as CustomTax,
                    RawData:zipTaxID::INTEGER as ZipTaxID,
                    RawData:smsReminders::INTEGER as SMSReminders,
                    RawData:phoneReminders::INTEGER as PhoneReminders,
                    RawData:emailReminders::INTEGER as EmailReminders,
                    RawData:customerSource::STRING as CustomerSource,
                    RawData:customerSourceID::INTEGER as CustomerSourceID,
                    RawData:maxMonthlyCharge::FLOAT as MaxMonthlyCharge,
                    RawData:county::STRING as County,
                    RawData:autopayPaymentProfileID::INTEGER as AutopayPaymentProfileID,
                    RawData:divisionID::INTEGER as DivisionID,
                    CASE WHEN RawData:agingDate::STRING IN ('0000-00-00', '', '0000-00-00 00:00:00') OR RawData:agingDate IS NULL 
                         THEN NULL ELSE TRY_TO_DATE(RawData:agingDate::STRING) END as AgingDate,
                    CASE WHEN RawData:responsibleAgingDate::STRING IN ('0000-00-00', '', '0000-00-00 00:00:00') OR RawData:responsibleAgingDate IS NULL 
                         THEN NULL ELSE TRY_TO_DATE(RawData:responsibleAgingDate::STRING) END as ResponsibleAgingDate,
                    RawData:salesmanAPay::STRING as SalesmanAPay,
                    RawData:termiteMonitoring::INTEGER as TermiteMonitoring,
                    RawData:pendingCancel::INTEGER as PendingCancel,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:customerID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.CUSTOMER_FACT
                {where_clause}
            ) src
            ON tgt.CustomerID = src.CustomerID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                BillToAccountID = src.BillToAccountID,
                OfficeID = src.OfficeID,
                FName = src.FName,
                LName = src.LName,
                CompanyName = src.CompanyName,
                Spouse = src.Spouse,
                IsCommercial = src.IsCommercial,
                Status = src.Status,
                StatusText = src.StatusText,
                Email = src.Email,
                Phone1 = src.Phone1,
                Phone1_Ext = src.Phone1_Ext,
                Phone2 = src.Phone2,
                Phone2_Ext = src.Phone2_Ext,
                Address = src.Address,
                City = src.City,
                State = src.State,
                Zip = src.Zip,
                BillingCompanyName = src.BillingCompanyName,
                BillingFName = src.BillingFName,
                BillingLName = src.BillingLName,
                BillingAddress = src.BillingAddress,
                BillingCity = src.BillingCity,
                BillingState = src.BillingState,
                BillingZip = src.BillingZip,
                BillingPhone = src.BillingPhone,
                BillingEmail = src.BillingEmail,
                Latitude = src.Latitude,
                Longitude = src.Longitude,
                SqFeet = src.SqFeet,
                AddedByEmployeeID = src.AddedByEmployeeID,
                DateAdded = src.DateAdded,
                DateCancelled = src.DateCancelled,
                DateUpdated = src.DateUpdated,
                SourceID = src.SourceID,
                Source = src.Source,
                APay = src.APay,
                PreferredTechID = src.PreferredTechID,
                PaidInFull = src.PaidInFull,
                SubscriptionIDs = src.SubscriptionIDs,
                Balance = src.Balance,
                BalanceAge = src.BalanceAge,
                ResponsibleBalance = src.ResponsibleBalance,
                ResponsibleBalanceAge = src.ResponsibleBalanceAge,
                MasterAccount = src.MasterAccount,
                PreferredBillingDate = src.PreferredBillingDate,
                PaymentHoldDate = src.PaymentHoldDate,
                LatestCCLastFour = src.LatestCCLastFour,
                LatestCCExpDate = src.LatestCCExpDate,
                AppointmentIDs = src.AppointmentIDs,
                TicketIDs = src.TicketIDs,
                PaymentIDs = src.PaymentIDs,
                RegionID = src.RegionID,
                SpecialScheduling = src.SpecialScheduling,
                TaxRate = src.TaxRate,
                StateTax = src.StateTax,
                CityTax = src.CityTax,
                CountyTax = src.CountyTax,
                DistrictTax = src.DistrictTax,
                DistrictTax1 = src.DistrictTax1,
                DistrictTax2 = src.DistrictTax2,
                DistrictTax3 = src.DistrictTax3,
                DistrictTax4 = src.DistrictTax4,
                DistrictTax5 = src.DistrictTax5,
                CustomTax = src.CustomTax,
                ZipTaxID = src.ZipTaxID,
                SMSReminders = src.SMSReminders,
                PhoneReminders = src.PhoneReminders,
                EmailReminders = src.EmailReminders,
                CustomerSource = src.CustomerSource,
                CustomerSourceID = src.CustomerSourceID,
                MaxMonthlyCharge = src.MaxMonthlyCharge,
                County = src.County,
                AutopayPaymentProfileID = src.AutopayPaymentProfileID,
                DivisionID = src.DivisionID,
                AgingDate = src.AgingDate,
                ResponsibleAgingDate = src.ResponsibleAgingDate,
                SalesmanAPay = src.SalesmanAPay,
                TermiteMonitoring = src.TermiteMonitoring,
                PendingCancel = src.PendingCancel,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                CustomerID, BillToAccountID, OfficeID, FName, LName, CompanyName, Spouse,
                IsCommercial, Status, StatusText, Email, Phone1, Phone1_Ext, Phone2, Phone2_Ext,
                Address, City, State, Zip, BillingCompanyName, BillingFName, BillingLName,
                BillingAddress, BillingCity, BillingState, BillingZip, BillingPhone, BillingEmail,
                Latitude, Longitude, SqFeet, AddedByEmployeeID, DateAdded, DateCancelled, DateUpdated,
                SourceID, Source, APay, PreferredTechID, PaidInFull, SubscriptionIDs,
                Balance, BalanceAge, ResponsibleBalance, ResponsibleBalanceAge, MasterAccount,
                PreferredBillingDate, PaymentHoldDate, LatestCCLastFour, LatestCCExpDate,
                AppointmentIDs, TicketIDs, PaymentIDs, RegionID, SpecialScheduling,
                TaxRate, StateTax, CityTax, CountyTax, DistrictTax, DistrictTax1, DistrictTax2,
                DistrictTax3, DistrictTax4, DistrictTax5, CustomTax, ZipTaxID,
                SMSReminders, PhoneReminders, EmailReminders, CustomerSource, CustomerSourceID,
                MaxMonthlyCharge, County, AutopayPaymentProfileID, DivisionID,
                AgingDate, ResponsibleAgingDate, SalesmanAPay, TermiteMonitoring, PendingCancel,
                LoadDatetimeUTC
            ) VALUES (
                src.CustomerID, src.BillToAccountID, src.OfficeID, src.FName, src.LName, src.CompanyName, src.Spouse,
                src.IsCommercial, src.Status, src.StatusText, src.Email, src.Phone1, src.Phone1_Ext, src.Phone2, src.Phone2_Ext,
                src.Address, src.City, src.State, src.Zip, src.BillingCompanyName, src.BillingFName, src.BillingLName,
                src.BillingAddress, src.BillingCity, src.BillingState, src.BillingZip, src.BillingPhone, src.BillingEmail,
                src.Latitude, src.Longitude, src.SqFeet, src.AddedByEmployeeID, src.DateAdded, src.DateCancelled, src.DateUpdated,
                src.SourceID, src.Source, src.APay, src.PreferredTechID, src.PaidInFull, src.SubscriptionIDs,
                src.Balance, src.BalanceAge, src.ResponsibleBalance, src.ResponsibleBalanceAge, src.MasterAccount,
                src.PreferredBillingDate, src.PaymentHoldDate, src.LatestCCLastFour, src.LatestCCExpDate,
                src.AppointmentIDs, src.TicketIDs, src.PaymentIDs, src.RegionID, src.SpecialScheduling,
                src.TaxRate, src.StateTax, src.CityTax, src.CountyTax, src.DistrictTax, src.DistrictTax1, src.DistrictTax2,
                src.DistrictTax3, src.DistrictTax4, src.DistrictTax5, src.CustomTax, src.ZipTaxID,
                src.SMSReminders, src.PhoneReminders, src.EmailReminders, src.CustomerSource, src.CustomerSourceID,
                src.MaxMonthlyCharge, src.County, src.AutopayPaymentProfileID, src.DivisionID,
                src.AgingDate, src.ResponsibleAgingDate, src.SalesmanAPay, src.TermiteMonitoring, src.PendingCancel,
                src.LoadDatetimeUTC
            )
        """,
        
        "FACT_EMPLOYEE": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_EMPLOYEE tgt
            USING (
                SELECT 
                    RawData:employeeID::INTEGER as EmployeeID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:active::BOOLEAN as IsActive,
                    RawData:fname::STRING as FName,
                    RawData:lname::STRING as LName,
                    RawData:type::STRING as EmployeeTypeText,
                    RawData:phone::STRING as Phone,
                    RawData:email::STRING as Email,
                    RawData:experience::STRING as Experience,
                    RawData:skillIDs::VARIANT as SkillIDs,
                    RawData:skillDescriptions::VARIANT as SkillDescriptions,
                    RawData:linkedEmployeeIDs::VARIANT as LinkedEmployeeIDs,
                    RawData:employeeLink::STRING as EmployeeLink,
                    RawData:licenseNumber::STRING as LicenseNumber,
                    RawData:supervisorID::INTEGER as SupervisorID,
                    RawData:roamingRep::INTEGER as RoamingRep,
                    RawData:regionalManagerOfficeIDs::VARIANT as RegionalManagerOfficeIDs,
                    CASE WHEN RawData:lastLogin::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:lastLogin IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:lastLogin::STRING) END as LastLogin,
                    RawData:teamIDs::VARIANT as TeamIDs,
                    RawData:primaryTeam::STRING as PrimaryTeam,
                    RawData:accessControlProfileID::INTEGER as AccessControlProfileID,
                    RawData:startAddress::STRING as StartAddress,
                    RawData:startCity::STRING as StartCity,
                    RawData:startState::STRING as StartState,
                    RawData:startZip::STRING as StartZip,
                    RawData:startLat::FLOAT as StartLatitude,
                    RawData:startLng::FLOAT as StartLongitude,
                    RawData:endAddress::STRING as EndAddress,
                    RawData:endCity::STRING as EndCity,
                    RawData:endState::STRING as EndState,
                    RawData:endZip::STRING as EndZip,
                    RawData:endLat::FLOAT as EndLatitude,
                    RawData:endLng::FLOAT as EndLongitude,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:employeeID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.EMPLOYEE_FACT
                {where_clause}
            ) src
            ON tgt.EmployeeID = src.EmployeeID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                IsActive = src.IsActive,
                FName = src.FName,
                LName = src.LName,
                EmployeeTypeText = src.EmployeeTypeText,
                Phone = src.Phone,
                Email = src.Email,
                Experience = src.Experience,
                SkillIDs = src.SkillIDs,
                SkillDescriptions = src.SkillDescriptions,
                LinkedEmployeeIDs = src.LinkedEmployeeIDs,
                EmployeeLink = src.EmployeeLink,
                LicenseNumber = src.LicenseNumber,
                SupervisorID = src.SupervisorID,
                RoamingRep = src.RoamingRep,
                RegionalManagerOfficeIDs = src.RegionalManagerOfficeIDs,
                LastLogin = src.LastLogin,
                TeamIDs = src.TeamIDs,
                PrimaryTeam = src.PrimaryTeam,
                AccessControlProfileID = src.AccessControlProfileID,
                StartAddress = src.StartAddress,
                StartCity = src.StartCity,
                StartState = src.StartState,
                StartZip = src.StartZip,
                StartLatitude = src.StartLatitude,
                StartLongitude = src.StartLongitude,
                EndAddress = src.EndAddress,
                EndCity = src.EndCity,
                EndState = src.EndState,
                EndZip = src.EndZip,
                EndLatitude = src.EndLatitude,
                EndLongitude = src.EndLongitude,
                DateUpdated = src.DateUpdated,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                EmployeeID, OfficeID, IsActive, FName, LName, EmployeeTypeText, Phone, Email,
                Experience, SkillIDs, SkillDescriptions, LinkedEmployeeIDs, EmployeeLink,
                LicenseNumber, SupervisorID, RoamingRep, RegionalManagerOfficeIDs,
                LastLogin, TeamIDs, PrimaryTeam, AccessControlProfileID,
                StartAddress, StartCity, StartState, StartZip, StartLatitude, StartLongitude,
                EndAddress, EndCity, EndState, EndZip, EndLatitude, EndLongitude,
                DateUpdated, LoadDatetimeUTC
            ) VALUES (
                src.EmployeeID, src.OfficeID, src.IsActive, src.FName, src.LName, src.EmployeeTypeText, src.Phone, src.Email,
                src.Experience, src.SkillIDs, src.SkillDescriptions, src.LinkedEmployeeIDs, src.EmployeeLink,
                src.LicenseNumber, src.SupervisorID, src.RoamingRep, src.RegionalManagerOfficeIDs,
                src.LastLogin, src.TeamIDs, src.PrimaryTeam, src.AccessControlProfileID,
                src.StartAddress, src.StartCity, src.StartState, src.StartZip, src.StartLatitude, src.StartLongitude,
                src.EndAddress, src.EndCity, src.EndState, src.EndZip, src.EndLatitude, src.EndLongitude,
                src.DateUpdated, src.LoadDatetimeUTC
            )
        """,
        
        "FACT_TICKET": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_TICKET tgt
            USING (
                SELECT 
                    RawData:ticketID::INTEGER as TicketID,
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:billToAccountID::INTEGER as BillToAccountID,
                    RawData:officeID::INTEGER as OfficeID,
                    CASE WHEN RawData:dateCreated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateCreated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateCreated::STRING) END as DateCreated,
                    CASE WHEN RawData:invoiceDate::STRING IN ('0000-00-00', '', '0000-00-00 00:00:00') OR RawData:invoiceDate IS NULL 
                         THEN NULL ELSE TRY_TO_DATE(RawData:invoiceDate::STRING) END as InvoiceDate,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    RawData:active::BOOLEAN as IsActive,
                    RawData:subtotal::FLOAT as Subtotal,
                    RawData:taxAmount::FLOAT as TaxAmount,
                    RawData:total::FLOAT as Total,
                    RawData:serviceCharge::FLOAT as ServiceCharge,
                    RawData:serviceTaxable::INTEGER as ServiceTaxable,
                    RawData:productionValue::FLOAT as ProductionValue,
                    RawData:taxRate::FLOAT as TaxRate,
                    RawData:appointmentID::INTEGER as AppointmentID,
                    RawData:balance::FLOAT as Balance,
                    RawData:subscriptionID::INTEGER as SubscriptionID,
                    RawData:serviceID::INTEGER as ServiceID,
                    RawData:items::VARIANT as Items,
                    RawData:createdBy::INTEGER as CreatedByEmployeeID,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:ticketID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.TICKET_FACT
                {where_clause}
            ) src
            ON tgt.TicketID = src.TicketID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                CustomerID = src.CustomerID,
                BillToAccountID = src.BillToAccountID,
                OfficeID = src.OfficeID,
                DateCreated = src.DateCreated,
                InvoiceDate = src.InvoiceDate,
                DateUpdated = src.DateUpdated,
                IsActive = src.IsActive,
                Subtotal = src.Subtotal,
                TaxAmount = src.TaxAmount,
                Total = src.Total,
                ServiceCharge = src.ServiceCharge,
                ServiceTaxable = src.ServiceTaxable,
                ProductionValue = src.ProductionValue,
                TaxRate = src.TaxRate,
                AppointmentID = src.AppointmentID,
                Balance = src.Balance,
                SubscriptionID = src.SubscriptionID,
                ServiceID = src.ServiceID,
                Items = src.Items,
                CreatedByEmployeeID = src.CreatedByEmployeeID,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                TicketID, CustomerID, BillToAccountID, OfficeID,
                DateCreated, InvoiceDate, DateUpdated, IsActive,
                Subtotal, TaxAmount, Total, ServiceCharge, ServiceTaxable,
                ProductionValue, TaxRate, AppointmentID, Balance,
                SubscriptionID, ServiceID, Items, CreatedByEmployeeID,
                LoadDatetimeUTC
            ) VALUES (
                src.TicketID, src.CustomerID, src.BillToAccountID, src.OfficeID,
                src.DateCreated, src.InvoiceDate, src.DateUpdated, src.IsActive,
                src.Subtotal, src.TaxAmount, src.Total, src.ServiceCharge, src.ServiceTaxable,
                src.ProductionValue, src.TaxRate, src.AppointmentID, src.Balance,
                src.SubscriptionID, src.ServiceID, src.Items, src.CreatedByEmployeeID,
                src.LoadDatetimeUTC
            )
        """,
        
        "FACT_PAYMENT": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_PAYMENT tgt
            USING (
                SELECT 
                    RawData:paymentID::INTEGER as PaymentID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:customerID::INTEGER as CustomerID,
                    CASE WHEN RawData:date::STRING IN ('0000-00-00', '', '0000-00-00 00:00:00') OR RawData:date IS NULL 
                         THEN NULL ELSE TRY_TO_DATE(RawData:date::STRING) END as PaymentDate,
                    RawData:paymentMethod::STRING as PaymentMethod,
                    RawData:amount::FLOAT as Amount,
                    RawData:appliedAmount::FLOAT as AppliedAmount,
                    RawData:unassignedAmount::FLOAT as UnassignedAmount,
                    RawData:status::STRING as Status,
                    RawData:invoiceIDs::VARIANT as InvoiceIDs,
                    RawData:paymentApplications::VARIANT as PaymentApplications,
                    RawData:employeeID::INTEGER as EmployeeIDs,
                    RawData:officePayment::INTEGER as IsOfficePayment,
                    RawData:collectionPayment::INTEGER as IsCollectionPayment,
                    RawData:writeoff::INTEGER as Writeoff,
                    RawData:creditMemo::VARIANT as CreditMemo,
                    RawData:paymentOrigin::STRING as PaymentOrigin,
                    RawData:originalPaymentID::INTEGER as OriginalPaymentID,
                    RawData:lastFour::STRING as LastFour,
                    RawData:notes::STRING as PaymentNotes,
                    CASE WHEN RawData:batchOpened::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:batchOpened IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:batchOpened::STRING) END as BatchOpened,
                    CASE WHEN RawData:batchClosed::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:batchClosed IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:batchClosed::STRING) END as BatchClosed,
                    RawData:paymentSource::STRING as PaymentSource,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    RawData:transactionID::STRING as TransactionID,
                    RawData:subscriptionID::INTEGER as SubscriptionID,
                    RawData:cardType::STRING as CardType,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:paymentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.PAYMENT_FACT
                {where_clause}
            ) src
            ON tgt.PaymentID = src.PaymentID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                CustomerID = src.CustomerID,
                PaymentDate = src.PaymentDate,
                PaymentMethod = src.PaymentMethod,
                Amount = src.Amount,
                AppliedAmount = src.AppliedAmount,
                UnassignedAmount = src.UnassignedAmount,
                Status = src.Status,
                InvoiceIDs = src.InvoiceIDs,
                PaymentApplications = src.PaymentApplications,
                EmployeeIDs = src.EmployeeIDs,
                IsOfficePayment = src.IsOfficePayment,
                IsCollectionPayment = src.IsCollectionPayment,
                Writeoff = src.Writeoff,
                CreditMemo = src.CreditMemo,
                PaymentOrigin = src.PaymentOrigin,
                OriginalPaymentID = src.OriginalPaymentID,
                LastFour = src.LastFour,
                PaymentNotes = src.PaymentNotes,
                BatchOpened = src.BatchOpened,
                BatchClosed = src.BatchClosed,
                PaymentSource = src.PaymentSource,
                DateUpdated = src.DateUpdated,
                TransactionID = src.TransactionID,
                SubscriptionID = src.SubscriptionID,
                CardType = src.CardType,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                PaymentID, OfficeID, CustomerID, PaymentDate, PaymentMethod,
                Amount, AppliedAmount, UnassignedAmount, Status,
                InvoiceIDs, PaymentApplications, EmployeeIDs,
                IsOfficePayment, IsCollectionPayment, Writeoff, CreditMemo,
                PaymentOrigin, OriginalPaymentID, LastFour, PaymentNotes,
                BatchOpened, BatchClosed, PaymentSource, DateUpdated,
                TransactionID, SubscriptionID, CardType, LoadDatetimeUTC
            ) VALUES (
                src.PaymentID, src.OfficeID, src.CustomerID, src.PaymentDate, src.PaymentMethod,
                src.Amount, src.AppliedAmount, src.UnassignedAmount, src.Status,
                src.InvoiceIDs, src.PaymentApplications, src.EmployeeIDs,
                src.IsOfficePayment, src.IsCollectionPayment, src.Writeoff, src.CreditMemo,
                src.PaymentOrigin, src.OriginalPaymentID, src.LastFour, src.PaymentNotes,
                src.BatchOpened, src.BatchClosed, src.PaymentSource, src.DateUpdated,
                src.TransactionID, src.SubscriptionID, src.CardType, src.LoadDatetimeUTC
            )
        """,
        
        "FACT_APPLIED_PAYMENT": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT tgt
            USING (
                SELECT 
                    RawData:appliedPaymentID::INTEGER as AppliedPaymentID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:paymentID::INTEGER as PaymentID,
                    RawData:ticketID::INTEGER as TicketID,
                    RawData:customerID::INTEGER as CustomerID,
                    CASE WHEN RawData:dateApplied::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateApplied IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateApplied::STRING) END as DateApplied,
                    RawData:appliedBy::INTEGER as AppliedByEmployeeID,
                    RawData:appliedAmount::FLOAT as AppliedAmount,
                    RawData:taxCollected::FLOAT as TaxCollected,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:appliedPaymentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.APPLIEDPAYMENT_FACT
                {where_clause}
            ) src
            ON tgt.AppliedPaymentID = src.AppliedPaymentID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                PaymentID = src.PaymentID,
                TicketID = src.TicketID,
                CustomerID = src.CustomerID,
                DateApplied = src.DateApplied,
                AppliedByEmployeeID = src.AppliedByEmployeeID,
                AppliedAmount = src.AppliedAmount,
                TaxCollected = src.TaxCollected,
                DateUpdated = src.DateUpdated,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT (
                AppliedPaymentID, OfficeID, PaymentID, TicketID, CustomerID,
                DateApplied, AppliedByEmployeeID, AppliedAmount, TaxCollected,
                DateUpdated, LoadDatetimeUTC
            ) VALUES (
                src.AppliedPaymentID, src.OfficeID, src.PaymentID, src.TicketID, src.CustomerID,
                src.DateApplied, src.AppliedByEmployeeID, src.AppliedAmount, src.TaxCollected,
                src.DateUpdated, src.LoadDatetimeUTC
            )
        """
    }
    
    # First create tables if they don't exist
    create_statements = {
        "FACT_CUSTOMER": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_CUSTOMER (
                CustomerID INTEGER PRIMARY KEY,
                BillToAccountID INTEGER,
                OfficeID INTEGER,
                FName STRING,
                LName STRING,
                CompanyName STRING,
                Spouse STRING,
                IsCommercial INTEGER,
                Status STRING,
                StatusText STRING,
                Email STRING,
                Phone1 STRING,
                Phone1_Ext STRING,
                Phone2 STRING,
                Phone2_Ext STRING,
                Address STRING,
                City STRING,
                State STRING,
                Zip STRING,
                BillingCompanyName STRING,
                BillingFName STRING,
                BillingLName STRING,
                BillingAddress STRING,
                BillingCity STRING,
                BillingState STRING,
                BillingZip STRING,
                BillingPhone STRING,
                BillingEmail STRING,
                Latitude FLOAT,
                Longitude FLOAT,
                SqFeet INTEGER,
                AddedByEmployeeID INTEGER,
                DateAdded TIMESTAMP_NTZ,
                DateCancelled TIMESTAMP_NTZ,
                DateUpdated TIMESTAMP_NTZ,
                SourceID INTEGER,
                Source STRING,
                APay STRING,
                PreferredTechID INTEGER,
                PaidInFull INTEGER,
                SubscriptionIDs VARIANT,
                Balance FLOAT,
                BalanceAge INTEGER,
                ResponsibleBalance FLOAT,
                ResponsibleBalanceAge INTEGER,
                MasterAccount INTEGER,
                PreferredBillingDate STRING,
                PaymentHoldDate DATE,
                LatestCCLastFour STRING,
                LatestCCExpDate STRING,
                AppointmentIDs VARIANT,
                TicketIDs VARIANT,
                PaymentIDs VARIANT,
                RegionID INTEGER,
                SpecialScheduling STRING,
                TaxRate FLOAT,
                StateTax FLOAT,
                CityTax FLOAT,
                CountyTax FLOAT,
                DistrictTax FLOAT,
                DistrictTax1 FLOAT,
                DistrictTax2 FLOAT,
                DistrictTax3 FLOAT,
                DistrictTax4 FLOAT,
                DistrictTax5 FLOAT,
                CustomTax FLOAT,
                ZipTaxID INTEGER,
                SMSReminders INTEGER,
                PhoneReminders INTEGER,
                EmailReminders INTEGER,
                CustomerSource STRING,
                CustomerSourceID INTEGER,
                MaxMonthlyCharge FLOAT,
                County STRING,
                AutopayPaymentProfileID INTEGER,
                DivisionID INTEGER,
                AgingDate DATE,
                ResponsibleAgingDate DATE,
                SalesmanAPay STRING,
                TermiteMonitoring INTEGER,
                PendingCancel INTEGER,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        
        "FACT_EMPLOYEE": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_EMPLOYEE (
                EmployeeID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                IsActive BOOLEAN,
                FName STRING,
                LName STRING,
                EmployeeTypeText STRING,
                Phone STRING,
                Email STRING,
                Experience STRING,
                SkillIDs VARIANT,
                SkillDescriptions VARIANT,
                LinkedEmployeeIDs VARIANT,
                EmployeeLink STRING,
                LicenseNumber STRING,
                SupervisorID INTEGER,
                RoamingRep INTEGER,
                RegionalManagerOfficeIDs VARIANT,
                LastLogin TIMESTAMP_NTZ,
                TeamIDs VARIANT,
                PrimaryTeam STRING,
                AccessControlProfileID INTEGER,
                StartAddress STRING,
                StartCity STRING,
                StartState STRING,
                StartZip STRING,
                StartLatitude FLOAT,
                StartLongitude FLOAT,
                EndAddress STRING,
                EndCity STRING,
                EndState STRING,
                EndZip STRING,
                EndLatitude FLOAT,
                EndLongitude FLOAT,
                DateUpdated TIMESTAMP_NTZ,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        
        "FACT_TICKET": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_TICKET (
                TicketID INTEGER PRIMARY KEY,
                CustomerID INTEGER,
                BillToAccountID INTEGER,
                OfficeID INTEGER,
                DateCreated TIMESTAMP_NTZ,
                InvoiceDate DATE,
                DateUpdated TIMESTAMP_NTZ,
                IsActive BOOLEAN,
                Subtotal FLOAT,
                TaxAmount FLOAT,
                Total FLOAT,
                ServiceCharge FLOAT,
                ServiceTaxable INTEGER,
                ProductionValue FLOAT,
                TaxRate FLOAT,
                AppointmentID INTEGER,
                Balance FLOAT,
                SubscriptionID INTEGER,
                ServiceID INTEGER,
                Items VARIANT,
                CreatedByEmployeeID INTEGER,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        
        "FACT_PAYMENT": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_PAYMENT (
                PaymentID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                CustomerID INTEGER,
                PaymentDate DATE,
                PaymentMethod STRING,
                Amount FLOAT,
                AppliedAmount FLOAT,
                UnassignedAmount FLOAT,
                Status STRING,
                InvoiceIDs VARIANT,
                PaymentApplications VARIANT,
                EmployeeIDs INTEGER,
                IsOfficePayment INTEGER,
                IsCollectionPayment INTEGER,
                Writeoff INTEGER,
                CreditMemo VARIANT,
                PaymentOrigin STRING,
                OriginalPaymentID INTEGER,
                LastFour STRING,
                PaymentNotes STRING,
                BatchOpened TIMESTAMP_NTZ,
                BatchClosed TIMESTAMP_NTZ,
                PaymentSource STRING,
                DateUpdated TIMESTAMP_NTZ,
                TransactionID STRING,
                SubscriptionID INTEGER,
                CardType STRING,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        
        "FACT_APPLIED_PAYMENT": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_APPLIED_PAYMENT (
                AppliedPaymentID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                PaymentID INTEGER,
                TicketID INTEGER,
                CustomerID INTEGER,
                DateApplied TIMESTAMP_NTZ,
                AppliedByEmployeeID INTEGER,
                AppliedAmount FLOAT,
                TaxCollected FLOAT,
                DateUpdated TIMESTAMP_NTZ,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """
    }
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE ALTAPESTANALYTICS")  # Add warehouse selection
            # Create tables first
            for table_name, create_sql in create_statements.items():
                cursor.execute(create_sql)
                logger.info(f"Ensured {table_name} exists")
            
            # Then run merges
            for table_name, merge_sql in fact_transformations.items():
                logger.info(f"Transforming {table_name}")
                cursor.execute(merge_sql)
                logger.info(f"Completed {table_name} transformation")


@task(name="transform_additional_fact_tables")
def transform_additional_fact_tables(incremental: bool = True) -> None:
    """Transform additional fact tables including appointment, route, note, task"""
    logger = get_run_logger()
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    
    where_clause = """
        WHERE LoadDatetimeUTC >= DATEADD(hour, -48, CURRENT_TIMESTAMP())
    """ if incremental else ""
    
    # Additional fact table transformations
    additional_transformations = {
        "FACT_APPOINTMENT": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_APPOINTMENT tgt
            USING (
                SELECT 
                    RawData:appointmentID::INTEGER as AppointmentID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:subscriptionID::INTEGER as SubscriptionID,
                    RawData:subscriptionRegionID::INTEGER as SubscriptionRegionID,
                    RawData:routeID::INTEGER as RouteID,
                    RawData:spotID::INTEGER as SpotID,
                    CASE WHEN RawData:date::STRING IN ('0000-00-00', '', '0000-00-00 00:00:00') OR RawData:date IS NULL 
                         THEN NULL ELSE TRY_TO_DATE(RawData:date::STRING) END as AppointmentDate,
                    CASE WHEN RawData:start IS NULL OR RawData:start::STRING = '' 
                         THEN NULL ELSE TIME(TRY_TO_TIMESTAMP_NTZ(RawData:start::STRING)) END as StartTime,
                    CASE WHEN RawData:end IS NULL OR RawData:end::STRING = '' 
                         THEN NULL ELSE TIME(TRY_TO_TIMESTAMP_NTZ(RawData:end::STRING)) END as EndTime,
                    RawData:timeWindow::STRING as TimeWindow,
                    RawData:duration::INTEGER as Duration,
                    RawData:type::STRING as AppointmentType,
                    CASE WHEN RawData:dateAdded::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateAdded IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateAdded::STRING) END as DateAdded,
                    RawData:employeeID::INTEGER as EmployeeID,
                    RawData:status::STRING as Status,
                    RawData:statusText::STRING as StatusText,
                    RawData:callAhead::INTEGER as CallAhead,
                    RawData:isInitial::BOOLEAN as IsInitial,
                    RawData:subscriptionPreferredTech::INTEGER as SubscriptionPreferredTech,
                    RawData:completedBy::INTEGER as CompletedBy,
                    RawData:servicedBy::STRING as ServicedBy,
                    CASE WHEN RawData:dateCompleted::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateCompleted IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateCompleted::STRING) END as DateCompleted,
                    RawData:notes::STRING as AppointmentNotes,
                    RawData:officeNotes::STRING as OfficeNotes,
                    CASE WHEN RawData:TimeIn::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:TimeIn IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:TimeIn::STRING) END as TimeIn,
                    CASE WHEN RawData:TimeOut::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:TimeOut IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:TimeOut::STRING) END as TimeOut,
                    RawData:checkIn::STRING as CheckIn,
                    RawData:checkOut::STRING as CheckOut,
                    RawData:windSpeed::FLOAT as WindSpeed,
                    RawData:windDirection::STRING as WindDirection,
                    RawData:temperature::FLOAT as Temperature,
                    RawData:servicedInterior::INTEGER as IsInterior,
                    RawData:ticketID::INTEGER as TicketID,
                    CASE WHEN RawData:dateCancelled::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateCancelled IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateCancelled::STRING) END as DateCancelled,
                    RawData:additionalTechs::VARIANT as AdditionalTechs,
                    RawData:appointmentCancellationReason::STRING as AppointmentCancelReason,
                    RawData:cancellationReason::STRING as CancellationReason,
                    RawData:cancellationReasonID::INTEGER as CancellationReasonID,
                    RawData:rescheduleReasonID::INTEGER as RescheduleReasonID,
                    RawData:reserviceReasonID::INTEGER as ReserviceReasonID,
                    RawData:targetPests::VARIANT as TargetPests,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:appointmentID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.APPOINTMENT_FACT
                {where_clause}
            ) src
            ON tgt.AppointmentID = src.AppointmentID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                CustomerID = src.CustomerID,
                SubscriptionID = src.SubscriptionID,
                SubscriptionRegionID = src.SubscriptionRegionID,
                RouteID = src.RouteID,
                SpotID = src.SpotID,
                AppointmentDate = src.AppointmentDate,
                StartTime = src.StartTime,
                EndTime = src.EndTime,
                TimeWindow = src.TimeWindow,
                Duration = src.Duration,
                AppointmentType = src.AppointmentType,
                DateAdded = src.DateAdded,
                EmployeeID = src.EmployeeID,
                Status = src.Status,
                StatusText = src.StatusText,
                CallAhead = src.CallAhead,
                IsInitial = src.IsInitial,
                SubscriptionPreferredTech = src.SubscriptionPreferredTech,
                CompletedBy = src.CompletedBy,
                ServicedBy = src.ServicedBy,
                DateCompleted = src.DateCompleted,
                AppointmentNotes = src.AppointmentNotes,
                OfficeNotes = src.OfficeNotes,
                TimeIn = src.TimeIn,
                TimeOut = src.TimeOut,
                CheckIn = src.CheckIn,
                CheckOut = src.CheckOut,
                WindSpeed = src.WindSpeed,
                WindDirection = src.WindDirection,
                Temperature = src.Temperature,
                IsInterior = src.IsInterior,
                TicketID = src.TicketID,
                DateCancelled = src.DateCancelled,
                AdditionalTechs = src.AdditionalTechs,
                AppointmentCancelReason = src.AppointmentCancelReason,
                CancellationReason = src.CancellationReason,
                CancellationReasonID = src.CancellationReasonID,
                RescheduleReasonID = src.RescheduleReasonID,
                ReserviceReasonID = src.ReserviceReasonID,
                TargetPests = src.TargetPests,
                DateUpdated = src.DateUpdated,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT VALUES (
                src.AppointmentID, src.OfficeID, src.CustomerID, src.SubscriptionID, src.SubscriptionRegionID,
                src.RouteID, src.SpotID, src.AppointmentDate, src.StartTime, src.EndTime,
                src.TimeWindow, src.Duration, src.AppointmentType, src.DateAdded, src.EmployeeID,
                src.Status, src.StatusText, src.CallAhead, src.IsInitial, src.SubscriptionPreferredTech,
                src.CompletedBy, src.ServicedBy, src.DateCompleted, src.AppointmentNotes, src.OfficeNotes,
                src.TimeIn, src.TimeOut, src.CheckIn, src.CheckOut, src.WindSpeed, src.WindDirection,
                src.Temperature, src.IsInterior, src.TicketID, src.DateCancelled, src.AdditionalTechs,
                src.AppointmentCancelReason, src.CancellationReason, src.CancellationReasonID,
                src.RescheduleReasonID, src.ReserviceReasonID, src.TargetPests, src.DateUpdated,
                src.LoadDatetimeUTC
            )
        """,
        
        "FACT_NOTE": f"""
            MERGE INTO STAGING_DB.FIELDROUTES.FACT_NOTE tgt
            USING (
                SELECT 
                    RawData:noteID::INTEGER as NoteID,
                    RawData:officeID::INTEGER as OfficeID,
                    RawData:customerID::INTEGER as CustomerID,
                    RawData:customerName::STRING as CustomerName,
                    RawData:customerSpouse::STRING as CustomerSpouse,
                    RawData:companyName::STRING as CompanyName,
                    RawData:employeeID::INTEGER as EmployeeID,
                    RawData:employeeName::STRING as EmployeeName,
                    CASE WHEN RawData:date::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:date IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:date::STRING) END as DateCreated,
                    RawData:showCustomer::INTEGER as IsVisibleCustomer,
                    RawData:showTech::INTEGER as IsVisibleTechnician,
                    RawData:cancellationReasonID::INTEGER as CancellationReasonID,
                    RawData:cancellationReason::STRING as CancellationReason,
                    RawData:typeID::INTEGER as TypeID,
                    RawData:type::STRING as TypeDescription,
                    RawData:contactTypeCategories::VARIANT as ContactTypeCategories,
                    RawData:notes::STRING as Text,
                    RawData:referenceID::INTEGER as ReferenceID,
                    CASE WHEN RawData:dateAdded::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateAdded IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateAdded::STRING) END as DateAdded,
                    CASE WHEN RawData:dateUpdated::STRING IN ('0000-00-00 00:00:00', '', '0000-00-00') OR RawData:dateUpdated IS NULL 
                         THEN NULL ELSE TRY_TO_TIMESTAMP_NTZ(RawData:dateUpdated::STRING) END as DateUpdated,
                    RawData:openCount::INTEGER as OpenCount,
                    RawData:clicksCount::INTEGER as ClickCount,
                    RawData:emailStatus::STRING as EmailStatus,
                    LoadDatetimeUTC,
                    ROW_NUMBER() OVER (PARTITION BY RawData:noteID::INTEGER ORDER BY LoadDatetimeUTC DESC) as rn
                FROM RAW_DB.FIELDROUTES.NOTE_FACT
                {where_clause}
            ) src
            ON tgt.NoteID = src.NoteID
            WHEN MATCHED AND src.rn = 1 THEN UPDATE SET
                OfficeID = src.OfficeID,
                CustomerID = src.CustomerID,
                CustomerName = src.CustomerName,
                CustomerSpouse = src.CustomerSpouse,
                CompanyName = src.CompanyName,
                EmployeeID = src.EmployeeID,
                EmployeeName = src.EmployeeName,
                DateCreated = src.DateCreated,
                IsVisibleCustomer = src.IsVisibleCustomer,
                IsVisibleTechnician = src.IsVisibleTechnician,
                CancellationReasonID = src.CancellationReasonID,
                CancellationReason = src.CancellationReason,
                TypeID = src.TypeID,
                TypeDescription = src.TypeDescription,
                ContactTypeCategories = src.ContactTypeCategories,
                Text = src.Text,
                ReferenceID = src.ReferenceID,
                DateAdded = src.DateAdded,
                DateUpdated = src.DateUpdated,
                OpenCount = src.OpenCount,
                ClickCount = src.ClickCount,
                EmailStatus = src.EmailStatus,
                LoadDatetimeUTC = src.LoadDatetimeUTC
            WHEN NOT MATCHED AND src.rn = 1 THEN INSERT VALUES (
                src.NoteID, src.OfficeID, src.CustomerID, src.CustomerName, src.CustomerSpouse,
                src.CompanyName, src.EmployeeID, src.EmployeeName, src.DateCreated,
                src.IsVisibleCustomer, src.IsVisibleTechnician, src.CancellationReasonID,
                src.CancellationReason, src.TypeID, src.TypeDescription, src.ContactTypeCategories,
                src.Text, src.ReferenceID, src.DateAdded, src.DateUpdated, src.OpenCount,
                src.ClickCount, src.EmailStatus, src.LoadDatetimeUTC
            )
        """
    }
    
    # Create table statements for additional tables
    additional_creates = {
        "FACT_APPOINTMENT": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_APPOINTMENT (
                AppointmentID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                CustomerID INTEGER,
                SubscriptionID INTEGER,
                SubscriptionRegionID INTEGER,
                RouteID INTEGER,
                SpotID INTEGER,
                AppointmentDate DATE,
                StartTime TIME,
                EndTime TIME,
                TimeWindow STRING,
                Duration INTEGER,
                AppointmentType STRING,
                DateAdded TIMESTAMP_NTZ,
                EmployeeID INTEGER,
                Status STRING,
                StatusText STRING,
                CallAhead INTEGER,
                IsInitial BOOLEAN,
                SubscriptionPreferredTech INTEGER,
                CompletedBy INTEGER,
                ServicedBy STRING,
                DateCompleted TIMESTAMP_NTZ,
                AppointmentNotes STRING,
                OfficeNotes STRING,
                TimeIn TIME,
                TimeOut TIME,
                CheckIn STRING,
                CheckOut STRING,
                WindSpeed FLOAT,
                WindDirection STRING,
                Temperature FLOAT,
                IsInterior BOOLEAN,
                TicketID INTEGER,
                DateCancelled TIMESTAMP_NTZ,
                AdditionalTechs VARIANT,
                AppointmentCancelReason STRING,
                CancellationReason STRING,
                CancellationReasonID INTEGER,
                RescheduleReasonID INTEGER,
                ReserviceReasonID INTEGER,
                TargetPests VARIANT,
                DateUpdated TIMESTAMP_NTZ,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """,
        
        "FACT_NOTE": """
            CREATE TABLE IF NOT EXISTS STAGING_DB.FIELDROUTES.FACT_NOTE (
                NoteID INTEGER PRIMARY KEY,
                OfficeID INTEGER,
                CustomerID INTEGER,
                CustomerName STRING,
                CustomerSpouse STRING,
                CompanyName STRING,
                EmployeeID INTEGER,
                EmployeeName STRING,
                DateCreated TIMESTAMP_NTZ,
                IsVisibleCustomer BOOLEAN,
                IsVisibleTechnician BOOLEAN,
                CancellationReasonID INTEGER,
                CancellationReason STRING,
                TypeID INTEGER,
                TypeDescription STRING,
                ContactTypeCategories VARIANT,
                Text STRING,
                ReferenceID INTEGER,
                DateAdded TIMESTAMP_NTZ,
                DateUpdated TIMESTAMP_NTZ,
                OpenCount INTEGER,
                ClickCount INTEGER,
                EmailStatus STRING,
                LoadDatetimeUTC TIMESTAMP_NTZ
            )
        """
    }
    
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE ALTAPESTANALYTICS")  # Add warehouse selection
            # Create tables first
            for table_name, create_sql in additional_creates.items():
                cursor.execute(create_sql)
                logger.info(f"Ensured {table_name} exists")
            
            # Then run merges
            for table_name, merge_sql in additional_transformations.items():
                logger.info(f"Transforming {table_name}")
                cursor.execute(merge_sql)
                logger.info(f"Completed {table_name} transformation")


@task(name="refresh_reporting_views")
def refresh_reporting_views() -> None:
    """Refresh materialized views if needed"""
    logger = get_run_logger()
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    
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
            cursor.execute("USE WAREHOUSE ALTAPESTANALYTICS")  # Add warehouse selection
            cursor.execute("USE DATABASE PRODUCTION_DB")
            cursor.execute("USE SCHEMA FIELDROUTES")
            
            for view_name in critical_views:
                # Check if view exists
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.VIEWS 
                    WHERE TABLE_SCHEMA = 'FIELDROUTES' 
                    AND TABLE_NAME = '{view_name}'
                """)
                
                if cursor.fetchone()[0] > 0:
                    logger.info(f"View {view_name} exists and is ready for use")
                else:
                    logger.warning(f"View {view_name} not found - may need to run create_reporting_views_production.sql")


@task(name="validate_data_quality")
def validate_data_quality() -> Dict[str, any]:
    """Run data quality checks on transformed data"""
    logger = get_run_logger()
    snowflake = SnowflakeConnector.load("snowflake-altapestdb")
    
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
            WHERE DateCreated > CURRENT_TIMESTAMP()
        """
    }
    
    results = {}
    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE ALTAPESTANALYTICS")  # Add warehouse selection
            for check_name, query in quality_checks.items():
                try:
                    cursor.execute(query)
                    result = cursor.fetchone()[0]
                    results[check_name] = result
                    
                    if result > 0:
                        logger.warning(f"Data quality issue: {check_name} = {result}")
                    else:
                        logger.info(f"Data quality check passed: {check_name}")
                except Exception as e:
                    logger.error(f"Quality check {check_name} failed: {str(e)}")
                    results[check_name] = "ERROR"
    
    return results


@flow(name="transform-raw-to-staging-complete")
def transform_raw_to_staging(
    incremental: bool = True,
    run_quality_checks: bool = True
):
    """
    Transform raw FieldRoutes data to staging tables with all columns from FR_SF_Lookup.csv
    
    Args:
        incremental: If True, only process recent data. If False, full reload.
        run_quality_checks: If True, validate data quality after transformation.
    """
    logger = get_run_logger()
    logger.info(f"Starting transformation - Mode: {'Incremental' if incremental else 'Full'}")
    
    # Create staging schema
    create_staging_schema()
    
    # Transform dimension tables (always full refresh for dims)
    transform_dimension_tables()
    
    # Transform core fact tables
    transform_fact_tables(incremental=incremental)
    
    # Transform additional fact tables
    transform_additional_fact_tables(incremental=incremental)
    
    # Refresh/validate reporting views
    refresh_reporting_views()
    
    # Run data quality checks
    if run_quality_checks:
        quality_results = validate_data_quality()
        logger.info(f"Data quality check results: {quality_results}")
    
    logger.info("Transformation completed successfully")


if __name__ == "__main__":
    # Run full transformation
    transform_raw_to_staging(incremental=False, run_quality_checks=True)