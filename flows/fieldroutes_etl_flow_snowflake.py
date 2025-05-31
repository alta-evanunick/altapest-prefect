from __future__ import annotations
"""
FieldRoutes → Snowflake ETL - Direct Load Version
Bypasses Azure Blob Storage and writes directly to Snowflake
"""
import json
import time
import datetime
from datetime import timezone
from typing import Dict, List, Optional, Tuple
import requests
import pytz
try:
    import orjson
    HAS_ORJSON = True
except ImportError:
    import json
    HAS_ORJSON = False
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector

# =============================================================================
# Entity metadata configuration
# =============================================================================
# Format: (endpoint, table_name, is_dim, is_small, primary_date, secondary_date, unique_params)
ENTITY_META = [
    # Dimension tables (reference data)
    ("region",         "REGION_DIM",          True,   True, None, None, {}),
    ("serviceType",    "SERVICETYPE_DIM",     True,   True, None, None, {}),  # Not in CSV, keeping legacy
    ("customerSource", "CUSTOMERSOURCE_DIM",  True,   True, None, None, {}),  # Not in CSV, keeping legacy
    ("genericFlag",    "GENERICFLAG_DIM",     True,   True, None, None, {}),
    ("cancellationReason", "CANCELLATIONREASON_DIM", True, True, None, None, {}),
    ("product",        "PRODUCT_DIM",         True,   True, None, None, {}),
    ("reserviceReason", "RESERVICEREASON_DIM", True,  True, None, None, {}),
    
    # Fact tables (transactional data) - Aligned with CSV
    ("customer",       "CUSTOMER_FACT",       False, False, "dateUpdated", "dateAdded", {"includeCancellationReason": 1}),
    ("employee",       "EMPLOYEE_FACT",       False, False, "dateUpdated", None, {}),
    ("appointment",    "APPOINTMENT_FACT",    False, False, "dateUpdated", "dateAdded", {"includeCancellationReason": 1, "includeTargetPests": 1}),
    ("subscription",   "SUBSCRIPTION_FACT",   False, False, "dateUpdated", "dateAdded", {}),
    ("route",          "ROUTE_FACT",          False, False, "dateUpdated", "date", {}),
    ("ticket",         "TICKET_FACT",         False, False, "dateUpdated", "dateCreated", {}),
    ("ticketItem",     "TICKETITEM_FACT",     False, False, "dateUpdated", "dateCreated", {}),
    ("payment",        "PAYMENT_FACT",        False, False, "dateUpdated", "date", {}),
    ("appliedPayment", "APPLIEDPAYMENT_FACT", False, False, "dateUpdated", "dateApplied", {}),
    ("note",           "NOTE_FACT",           False, False, "dateUpdated", "dateAdded", {}),
    ("task",           "TASK_FACT",           False, False, "dateUpdated", "dateAdded", {}),
    ("door",           "DOOR_FACT",      False, False, "timeCreated", None, {}),
    ("disbursement",   "DISBURSEMENT_FACT", False, False, "dateUpdated", "dateCreated", {}),
    ("chargeback",     "CHARGEBACK_FACT", False, False, "dateUpdated", "dateCreated", {}),
    ("additionalContacts", "ADDITIONALCONTACTS_FACT", False, False, "dateUpdated", "dateCreated", {}),
    ("disbursementItem", "DISBURSEMENTITEM_FACT", False, False, "dateUpdated", "dateCreated", {}),
    ("genericFlagAssignment", "GENERICFLAGASSIGNMENT_FACT", False, False, "dateUpdated", "dateCreated", {}),
    ("knock",          "KNOCK_FACT",          False, False, "dateUpdated", "dateAdded", {}),
    ("paymentProfile", "PAYMENTPROFILE_FACT", False, False, "dateUpdated", "dateCreated", {}),
    ("appointmentReminder", "APPOINTMENTREMINDER_FACT", False, False, "dateUpdated", None, {}),
]

# Helper functions
def chunk_list(items: List, chunk_size: int = 1000):
    """Split a list into chunks of specified size."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]

def make_api_request(url: str, headers: Dict, params: Dict = None, timeout: int = 30) -> Dict:
    """Make API request with retry logic."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            return response.json()
            
        except requests.HTTPError as e:
            if e.response and e.response.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            raise
        except (requests.ConnectionError, requests.Timeout):
            if attempt == max_retries - 1:
                raise
            time.sleep(2 ** attempt)
    
    raise RuntimeError(f"Failed after {max_retries} attempts")

# =============================================================================
# Main ETL Task - Direct to Snowflake Version
# =============================================================================
@task(
    name="fetch_entity_to_snowflake",
    description="Fetch data for one entity from one office and load directly to Snowflake",
    retries=2,
    retry_delay_seconds=60,
    tags=["api", "extract", "snowflake"]
)
def fetch_entity(
    office: Dict, 
    meta: Dict, 
    window_start: Optional[datetime.datetime] = None, 
    window_end: Optional[datetime.datetime] = None
) -> Tuple[int, int]:
    """
    Fetch entity data from FieldRoutes API and load directly to Snowflake.
    
    Returns:
        Tuple of (records_fetched, records_loaded)
    """
    logger = get_run_logger()
    
    # Extract metadata with error handling
    try:
        entity = meta["endpoint"]
        table_name = meta["table"]
        primary_date_field = meta.get("primary_date")
        secondary_date_field = meta.get("secondary_date")
        unique_params = meta.get("unique_params", {})
        is_dimension = meta.get("is_dim", False)
    except KeyError as e:
        logger.error(f"KeyError accessing meta dictionary: {e}")
        logger.error(f"Meta dict keys: {list(meta.keys())}")
        logger.error(f"Meta dict content: {meta}")
        raise
    
    logger.info(f"Starting fetch for {entity} - Office {office['office_id']} ({office['office_name']})")
    
    # API configuration
    base_url = office["base_url"]
    headers = {
        "AuthenticationKey": office["auth_key"],
        "AuthenticationToken": office["auth_token"],
    }
    
    # Build query parameters
    params = {
        "officeIDs": office["office_id"]
    }
    
    # Add any unique parameters for this entity
    params.update(unique_params)
    
    # Convert UTC to Pacific Time for date filtering
    pacific_tz = pytz.timezone('America/Los_Angeles')
    pt_start = None
    pt_end = None
    
    # Convert dates if provided
    if window_start and window_end:
        pt_start = window_start.astimezone(pacific_tz)
        pt_end = window_end.astimezone(pacific_tz)
    
    # Add date filter for incremental loads
    # Apply to fact tables and entities with date fields
    if window_start and window_end and primary_date_field:
        # For CDC, we need timestamps, not just dates
        # Check if this is a CDC run (window is less than 24 hours)
        is_cdc = (window_end - window_start).total_seconds() < 86400  # Less than 24 hours
        
        if is_cdc:
            # Use full timestamp format for CDC
            date_filter = {
                "operator": "BETWEEN",
                "value": [
                    pt_start.strftime("%Y-%m-%d %H:%M:%S"),
                    pt_end.strftime("%Y-%m-%d %H:%M:%S")
                ]
            }
        else:
            # Use date-only format for nightly runs
            date_filter = {
                "operator": "BETWEEN",
                "value": [
                    pt_start.strftime("%Y-%m-%d"),
                    pt_end.strftime("%Y-%m-%d")
                ]
            }
        
        params[primary_date_field] = json.dumps(date_filter)
        logger.info(f"Using date filter on {primary_date_field}: {date_filter}")
    
    # == Step 1: Search for IDs with pagination for 50K limit ==================
    search_url = f"{base_url}/{entity}/search"
    logger.info(f"Searching {entity} with params: {params}")
    
    all_ids = []
    last_id = None
    page_count = 0
    
    while True:
        page_count += 1
        current_params = params.copy()
        
        # Add pagination filter if we have a last_id
        if last_id is not None:
            # Get the primary key field name from entity metadata
            pk_field = {
                "region": "regionID",
                "serviceType": "serviceTypeID",
                "customerSource": "customerSourceID",
                "genericFlag": "genericFlagID",
                "cancellationReason": "cancellationReasonID",
                "product": "productID",
                "reserviceReason": "reserviceReasonID",
                "customer": "customerID",
                "employee": "employeeID",
                "appointment": "appointmentID",
                "subscription": "subscriptionID",
                "route": "routeID",
                "ticket": "ticketID",
                "ticketItem": "ticketItemID",
                "payment": "paymentID",
                "appliedPayment": "appliedPaymentID",
                "note": "noteID",
                "task": "taskID",
                "door": "doorID",
                "disbursement": "gatewayDisbursementID",
                "chargeback": "gatewayChargebackID",
                "additionalContacts": "additionalContactID",
                "disbursementItem": "gatewayDisbursementEntryID",
                "genericFlagAssignment": "genericFlagAssignmentID",
                "knock": "knockID",
                "paymentProfile": "paymentProfileID", 
                "office": "officeID",
                "appointmentReminder": "appointmentReminderID"
            }.get(entity, f"{entity}ID")
            
            current_params[pk_field] = json.dumps({
                "operator": ">",
                "value": last_id
            })
            logger.info(f"Page {page_count}: Searching for {pk_field} > {last_id}")
        
        try:
            search_data = make_api_request(search_url, headers, current_params)
            
            if page_count == 1:
                logger.info(f"Search response success: {search_data.get('success', False)}")
                logger.info(f"Total count: {search_data.get('count', 0)}")
            
            page_ids = []
            
            # The propertyName field tells us where the IDs are
            if "propertyName" in search_data:
                id_field = search_data["propertyName"]
                if id_field in search_data:
                    page_ids = search_data[id_field]
            else:
                # Fallback - try common patterns
                possible_fields = [
                    f"{entity}IDs",
                    f"{entity}IDsNoDataExported",
                    "ids"
                ]
                for field in possible_fields:
                    if field in search_data and search_data[field]:
                        page_ids = search_data[field]
                        break
            
            if not page_ids:
                logger.info(f"No more IDs found on page {page_count}")
                break
                
            all_ids.extend(page_ids)
            logger.info(f"Page {page_count}: Found {len(page_ids)} IDs (total so far: {len(all_ids)})")
            
            # Check if we hit the 50K limit and need to paginate
            if len(page_ids) >= 50000:
                # CRITICAL FIX: Ensure we're actually making progress
                if page_ids:
                    new_last_id = max(page_ids)
                    if last_id is not None and new_last_id <= last_id:
                        logger.error(f"Pagination not progressing! last_id={last_id}, new_last_id={new_last_id}")
                        break
                    last_id = new_last_id
                    logger.warning(f"Hit 50K limit on page {page_count}, will fetch next page starting after ID {last_id}")
                else:
                    logger.error("Got 50K limit but no IDs to paginate from")
                    break
                
                # Safety check: prevent infinite loops
                if page_count > 100:
                    logger.error(f"Stopping pagination after {page_count} pages to prevent infinite loop")
                    break
                continue
            else:
                # Less than 50K means we got all records
                break
                
        except Exception as e:
            logger.error(f"Search failed on page {page_count}: {str(e)}")
            if page_count == 1:
                raise  # Re-raise if first page fails
            else:
                break  # Stop pagination on error but keep what we have
        
    # For entities with secondary date fields, perform additional search
    if secondary_date_field and pt_start and pt_end:
        logger.info(f"Performing additional search on {secondary_date_field} for {entity}")
        
        params_created = params.copy()
        # Remove the primary date field
        if primary_date_field:
            params_created.pop(primary_date_field, None)
        # Add the secondary date field
        # Use the same date/timestamp format as primary date field
        if is_cdc:
            date_format = "%Y-%m-%d %H:%M:%S"
        else:
            date_format = "%Y-%m-%d"
            
        params_created[secondary_date_field] = json.dumps({
            "operator": "BETWEEN",
            "value": [
                pt_start.strftime(date_format),
                pt_end.strftime(date_format)
            ]
        })
        
        # Search for records created in the date range (with pagination)
        created_ids = []
        last_id = None
        page_count = 0
        
        while True:
            page_count += 1
            current_params = params_created.copy()
            
            if last_id is not None:
                pk_field = {
                    "customer": "customerID",
                    "employee": "employeeID",
                    "appointment": "appointmentID",
                    "subscription": "subscriptionID",
                    "route": "routeID",
                    "ticket": "ticketID",
                    "ticketItem": "ticketItemID",
                    "payment": "paymentID",
                    "appliedPayment": "appliedPaymentID",
                    "disbursement": "disbursementID",
                    "chargeback": "chargebackID"
                }.get(entity, f"{entity}ID")
                
                current_params[pk_field] = json.dumps({
                    "operator": ">",
                    "value": last_id
                })
            
            try:
                created_data = make_api_request(search_url, headers, current_params)
                
                page_ids = []
                if "propertyName" in created_data and created_data["propertyName"] in created_data:
                    id_field = created_data["propertyName"]
                    page_ids = created_data[id_field]
                
                if not page_ids:
                    break
                    
                created_ids.extend(page_ids)
                
                if len(page_ids) >= 50000:
                    # Same pagination fix as above
                    if page_ids:
                        new_last_id = max(page_ids)
                        if last_id is not None and new_last_id <= last_id:
                            logger.error(f"Secondary search pagination not progressing!")
                            break
                        last_id = new_last_id
                        logger.warning(f"Hit 50K limit on {secondary_date_field} search page {page_count}")
                    else:
                        break
                    
                    if page_count > 100:
                        logger.error(f"Stopping secondary pagination after {page_count} pages")
                        break
                    continue
                else:
                    break
                    
            except Exception as e:
                logger.warning(f"Failed to search by dateAdded: {e}")
                break
        
        if created_ids:
            logger.info(f"Found {len(created_ids)} additional IDs from dateAdded search")
            # Combine and deduplicate
            all_ids = list(set(all_ids + created_ids))
            logger.info(f"Total unique IDs after combining: {len(all_ids)}")
    
    if not all_ids:
        logger.warning(f"No IDs found for {entity} in office {office['office_id']}")
        return 0, 0
    
    logger.info(f"Total unique IDs to fetch: {len(all_ids)}")
    
    # == Step 2: Fetch actual records using /get endpoint ======================
    all_records = []
    
    for id_chunk in chunk_list(all_ids, 1000):
        # Build query string for batch get
        id_list = ",".join(str(id) for id in id_chunk)
        get_url = f"{base_url}/{entity}/get?{entity}IDs=[{id_list}]"
        logger.info(f"Fetching chunk of {len(id_chunk)} records")
        
        try:
            get_data = make_api_request(get_url, headers, timeout=60)
            
            # The response structure varies by entity
            chunk_records = []
            
            # First try the plural entity name
            plural_map = {
                "region": "regions",
                "serviceType": "serviceTypes",
                "customerSource": "customerSources",
                "genericFlag": "genericFlags",
                "cancellationReason": "cancellationReasons",
                "product": "products",
                "reserviceReason": "reserviceReasons",
                "customer": "customers",
                "employee": "employees",
                "office": "offices",
                "appointment": "appointments",
                "subscription": "subscriptions",
                "route": "routes",
                "ticket": "tickets",
                "ticketItem": "ticketItems",
                "payment": "payments",
                "appliedPayment": "appliedPayments",
                "note": "notes",
                "task": "tasks",
                "door": "doors",
                "disbursement": "disbursements",
                "chargeback": "chargebacks",
                "additionalContacts": "additionalContacts",
                "disbursementItem": "disbursementItems",
                "genericFlagAssignment": "genericFlagAssignments",
                "knock": "knocks",
                "paymentProfile": "paymentProfiles",
                "appointmentReminder": "appointmentReminders"
            }
            
            plural_name = plural_map.get(entity, entity + "s")
            
            if plural_name in get_data:
                chunk_records = get_data[plural_name]
                logger.info(f"Found records in '{plural_name}' field")
            elif "propertyNameData" in get_data:
                # Some endpoints use this structure
                if isinstance(get_data["propertyNameData"], dict):
                    for key, value in get_data["propertyNameData"].items():
                        if isinstance(value, list):
                            chunk_records = value
                            break
            elif isinstance(get_data, list):
                # Sometimes the response is just a list
                chunk_records = get_data
            
            if chunk_records:
                all_records.extend(chunk_records)
                logger.info(f"Retrieved {len(chunk_records)} records in this chunk")
            else:
                logger.warning(f"No records found in get response. Keys: {list(get_data.keys())}")
                
        except Exception as e:
            logger.error(f"Failed to fetch chunk: {str(e)}")
            # Continue with other chunks rather than failing entirely
            continue
    
    total_records = len(all_records)
    logger.info(f"Total records fetched for {entity}: {total_records}")
    
    if total_records == 0:
        logger.warning(f"No records retrieved for {entity} despite having {len(all_ids)} IDs")
        return len(all_ids), 0
    
    # == Step 3: Write directly to Snowflake ===============================
    load_timestamp = window_end.strftime("%Y-%m-%d %H:%M:%S") if window_end else datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    try:
        sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
        
        # Prepare data for bulk insert
        # Keep records as Python objects for Snowflake to handle
        data_rows = []
        for record in all_records:
            data_rows.append((
                office["office_id"],
                load_timestamp,
                json.dumps(record)  # JSON string
                ))
        
        # Bulk insert using Snowflake's executemany
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            
            # Clear any existing data for this office/entity/timestamp to prevent duplicates
            logger.info(f"Clearing existing data for office {office['office_id']} at timestamp {load_timestamp}")
            cursor.execute(f"""
                DELETE FROM RAW_DB.FIELDROUTES.{table_name}
                WHERE OfficeID = %s AND LoadDatetimeUTC = %s
            """, (office["office_id"], load_timestamp))
            
            # Insert using staging table approach to handle PARSE_JSON errors
            logger.info(f"Inserting {len(data_rows)} records into {table_name} using staging approach")
            
            # Create staging table if not exists
            staging_table = f"{table_name}_staging"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS RAW_DB.FIELDROUTES.{staging_table} (
                    OfficeID INTEGER,
                    LoadDatetimeUTC TIMESTAMP_NTZ,
                    RawDataString VARCHAR
                    {", TransactionType VARCHAR"}
                )
            """)
            
            # Clear staging table for this office
            cursor.execute(f"""
                DELETE FROM RAW_DB.FIELDROUTES.{staging_table} 
                WHERE OfficeID = %s
            """, (office["office_id"],))
            
            # Prepare insert SQL based on entity type
            insert_sql = f"""
                INSERT INTO RAW_DB.FIELDROUTES.{staging_table} 
                (OfficeID, LoadDatetimeUTC, RawDataString)
                VALUES (%s, %s, %s)
            """
            
            # Insert in smaller batches to avoid any size limits
            batch_size = 1000
            total_staged = 0
            
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                cursor.executemany(insert_sql, batch)
                total_staged += len(batch)
                logger.info(f"Staged batch {i//batch_size + 1}: {total_staged}/{len(data_rows)} records")
            
            # Now move from staging to final table with TRY_PARSE_JSON
            cursor.execute(f"""
                INSERT INTO RAW_DB.FIELDROUTES.{table_name} 
                (OfficeID, LoadDatetimeUTC, RawData)
                SELECT 
                    OfficeID,
                    LoadDatetimeUTC,
                    TRY_PARSE_JSON(RawDataString)
                FROM RAW_DB.FIELDROUTES.{staging_table}
                WHERE OfficeID = %s
                AND TRY_PARSE_JSON(RawDataString) IS NOT NULL
            """, (office["office_id"],))
            
            # Get count of successful inserts
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM RAW_DB.FIELDROUTES.{staging_table}
                WHERE OfficeID = %s
                AND TRY_PARSE_JSON(RawDataString) IS NULL
            """, (office["office_id"],))
            
            failed_count = cursor.fetchone()[0]
            if failed_count > 0:
                logger.warning(f"WARNING: {failed_count} records failed JSON parsing and were skipped")
                
                # Log a sample of failed records for debugging
                cursor.execute(f"""
                    SELECT LEFT(RawDataString, 200) 
                    FROM RAW_DB.FIELDROUTES.{staging_table}
                    WHERE OfficeID = %s
                    AND TRY_PARSE_JSON(RawDataString) IS NULL
                    LIMIT 5
                """, (office["office_id"],))
                
                sample_failures = cursor.fetchall()
                for i, (sample,) in enumerate(sample_failures):
                    logger.warning(f"Failed JSON sample {i+1}: {sample}")
            
            # Clean up staging table
            cursor.execute(f"""
                DELETE FROM RAW_DB.FIELDROUTES.{staging_table} 
                WHERE OfficeID = %s
            """, (office["office_id"],))
            
            total_inserted = total_staged - failed_count
            logger.info(f"Successfully inserted {total_inserted} records (skipped {failed_count} invalid JSON)")
            
            # Update watermark
            cursor.execute("""
                UPDATE RAW_DB.REF.office_entity_watermark 
                SET last_run_utc = %s, 
                    records_loaded = %s,
                    last_success_utc = CURRENT_TIMESTAMP(),
                    error_count = 0
                WHERE office_id = %s AND entity_name = %s
            """, (load_timestamp, len(data_rows), office["office_id"], entity))
            
            conn.commit()
            
        logger.info(f"Successfully loaded {len(data_rows)} records to Snowflake table {table_name}")
        return len(all_records), len(data_rows)

    except Exception as e:
        logger.error(f"Snowflake load failed for {entity}: {str(e)}")
        
        # Update error count in watermark
        try:
            with sf_connector.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE RAW_DB.REF.office_entity_watermark 
                    SET error_count = error_count + 1,
                        last_run_utc = %s
                    WHERE office_id = %s AND entity_name = %s
                """, (load_timestamp, office["office_id"], entity))
                conn.commit()
        except:
            pass  # Don't fail the whole task if we can't update the error count
            
        raise


# =============================================================================
# Customer Cancellation Reasons Aggregation Task
# =============================================================================
@task(
    name="process_customer_cancellation_reasons",
    description="Generate CustomerCancellationReasons fact table from customer data",
    tags=["transform", "snowflake"]
)
def process_customer_cancellation_reasons(offices: List[Dict], window_end: datetime.datetime):
    """
    Process customer cancellation reasons from the customer entity.
    This creates a fact table by flattening the cancellationReason array
    and joining with the cancellationReason dimension table.
    """
    logger = get_run_logger()
    logger.info("Processing customer cancellation reasons aggregation")
    
    try:
        sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create the target table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS RAW_DB.FIELDROUTES.CUSTOMERCANCELLATIONREASONS_FACT (
                    OfficeID INTEGER,
                    CustomerID INTEGER,
                    CancellationReasonID INTEGER,
                    CancellationReasonDescription VARCHAR,
                    DateCancelled TIMESTAMP_NTZ,
                    LoadDatetimeUTC TIMESTAMP_NTZ,
                    PRIMARY KEY (OfficeID, CustomerID, CancellationReasonID)
                )
            """)
            
            load_timestamp = window_end.strftime("%Y-%m-%d %H:%M:%S")
            
            for office in offices:
                logger.info(f"Processing cancellation reasons for office {office['office_id']}")
                
                # Clear existing data for this office at this timestamp
                cursor.execute("""
                    DELETE FROM RAW_DB.FIELDROUTES.CUSTOMERCANCELLATIONREASONS_FACT
                    WHERE OfficeID = %s AND LoadDatetimeUTC = %s
                """, (office['office_id'], load_timestamp))
                
                # Insert flattened cancellation reasons with dimension lookup
                cursor.execute("""
                    INSERT INTO RAW_DB.FIELDROUTES.CUSTOMERCANCELLATIONREASONS_FACT
                    (OfficeID, CustomerID, CancellationReasonID, CancellationReasonDescription, 
                     DateCancelled, LoadDatetimeUTC)
                    SELECT DISTINCT
                        c.OfficeID,
                        c.RawData:customerID::INTEGER as CustomerID,
                        cr.value::INTEGER as CancellationReasonID,
                        COALESCE(
                            crd.RawData:description::VARCHAR,
                            'Unknown Reason'
                        ) as CancellationReasonDescription,
                        c.RawData:dateCancelled::TIMESTAMP_NTZ as DateCancelled,
                        %s as LoadDatetimeUTC
                    FROM RAW_DB.FIELDROUTES.CUSTOMER_FACT c
                    CROSS JOIN LATERAL FLATTEN(
                        INPUT => c.RawData:cancellationReasonIDs,
                        OUTER => TRUE
                    ) cr
                    LEFT JOIN RAW_DB.FIELDROUTES.CANCELLATIONREASON_DIM crd
                        ON crd.OfficeID = c.OfficeID
                        AND crd.RawData:cancellationReasonID::INTEGER = cr.value::INTEGER
                    WHERE c.OfficeID = %s
                    AND c.LoadDatetimeUTC = (
                        SELECT MAX(LoadDatetimeUTC) 
                        FROM RAW_DB.FIELDROUTES.CUSTOMER_FACT 
                        WHERE OfficeID = %s
                    )
                    AND cr.value IS NOT NULL
                    AND c.RawData:cancellationReasonIDs IS NOT NULL
                    AND ARRAY_SIZE(c.RawData:cancellationReasonIDs) > 0
                """, (load_timestamp, office['office_id'], office['office_id']))
                
                # Get count of records processed
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM RAW_DB.FIELDROUTES.CUSTOMERCANCELLATIONREASONS_FACT
                    WHERE OfficeID = %s AND LoadDatetimeUTC = %s
                """, (office['office_id'], load_timestamp))
                
                count = cursor.fetchone()[0]
                logger.info(f"Created {count} cancellation reason records for office {office['office_id']}")
            
            conn.commit()
            logger.info("Customer cancellation reasons processing complete")
            
    except Exception as e:
        logger.error(f"Failed to process customer cancellation reasons: {str(e)}")
        raise


# =============================================================================
# Schema validation task
# =============================================================================
@task(name="validate_snowflake_schema", tags=["validation"])
def validate_snowflake_schema() -> bool:
    """Ensure all required tables exist in Snowflake."""
    logger = get_run_logger()
    
    required_tables = set(meta[1] for meta in ENTITY_META)
    
    try:
        sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'FIELDROUTES' 
                AND TABLE_CATALOG = 'RAW'
            """)
            existing_tables = {row[0].upper() for row in cursor.fetchall()}
        
        # Convert required tables to uppercase for comparison
        required_tables_upper = {t.upper() for t in required_tables}
        missing_tables = required_tables_upper - existing_tables
        
        if missing_tables:
            logger.error(f"Missing tables in Snowflake: {missing_tables}")
            return False
        
        logger.info("All required tables exist in Snowflake")
        return True
        
    except Exception as e:
        logger.error(f"Schema validation failed: {str(e)}")
        return False


# =============================================================================
# Main Flow
# =============================================================================
@flow(
    name="FieldRoutes_to_Snowflake_Direct",
    description="Direct ETL from FieldRoutes API to Snowflake (bypassing Azure)"
)
def run_fieldroutes_etl(
    office_filter: Optional[List[int]] = None,
    entity_filter: Optional[List[str]] = None,
    is_full_refresh: bool = False,
    window_hours: int = 24
):
    """
    Main ETL flow for FieldRoutes to Snowflake.
    
    Args:
        office_filter: List of office IDs to process (None = all active offices)
        entity_filter: List of entity names to process (None = all entities)
        is_full_refresh: If True, load all data; if False, incremental load
        window_hours: Hours to look back for incremental loads
    """
    logger = get_run_logger()
    
    # Calculate time window
    now = datetime.datetime.now(datetime.UTC)
    if is_full_refresh:
        window_start = None
        window_end = None
        logger.info("Running FULL REFRESH - no date filtering")
    else:
        window_start = now - datetime.timedelta(hours=window_hours)
        window_end = now
        logger.info(f"Running incremental load. Window: {window_start} to {window_end}")
    
    # Validate schema first
    if not validate_snowflake_schema():
        raise RuntimeError("Schema validation failed. Please ensure all tables exist.")
    
    # Get office configuration
    sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_connector.get_connection() as conn:
        cursor = conn.cursor()
        
        if office_filter:
            placeholders = ','.join(['%s'] * len(office_filter))
            query = f"""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW_DB.REF.offices_lookup
                WHERE active = TRUE AND office_id IN ({placeholders})
            """
            cursor.execute(query, office_filter)
        else:
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW_DB.REF.offices_lookup
                WHERE active = TRUE
            """)
        
        offices = []
        for office_id, name, url, key_blk, tok_blk in cursor.fetchall():
            offices.append({
                "office_id": office_id,
                "office_name": name,
                "base_url": url,
                "auth_key": Secret.load(key_blk).get(),
                "auth_token": Secret.load(tok_blk).get(),
            })
    
    # Filter entities if specified
    entities_to_process = []
    for meta in ENTITY_META:
        if entity_filter and meta[0] not in entity_filter:
            continue
            
        # Unpack metadata tuple - all tuples have exactly 7 elements
        entity_dict = {
            "endpoint": meta[0], 
            "table": meta[1], 
            "is_dim": meta[2], 
            "small": meta[3], 
            "primary_date": meta[4],
            "secondary_date": meta[5],
            "unique_params": meta[6]
        }
        
        # Debug log for troubleshooting
        logger.info(f"Entity {meta[0]} metadata: primary_date={entity_dict.get('primary_date')}, secondary_date={entity_dict.get('secondary_date')}, keys={list(entity_dict.keys())}")
        
        entities_to_process.append(entity_dict)
    
    logger.info(f"Processing {len(entities_to_process)} entities for {len(offices)} offices")
    
    # Process each office/entity combination
    total_fetched = 0
    total_loaded = 0
    failed_count = 0
    
    for office in offices:
        logger.info(f"🏢 Processing office {office['office_id']} ({office['office_name']})")
        
        for meta in entities_to_process:
            try:
                fetched, loaded = fetch_entity(
                    office=office,
                    meta=meta,
                    window_start=window_start,
                    window_end=window_end
                )
                total_fetched += fetched
                total_loaded += loaded
                
                logger.info(f"✅ {meta['endpoint']} completed: {fetched} fetched, {loaded} loaded")
                
            except Exception as e:
                logger.error(f"{meta['endpoint']} failed for office {office['office_id']}: {str(e)}")
                failed_count += 1
    
    # Process customer cancellation reasons aggregation if customer entity was processed
    if not entity_filter or "customer" in entity_filter:
        try:
            logger.info("Processing customer cancellation reasons aggregation")
            process_customer_cancellation_reasons(offices, window_end or now)
        except Exception as e:
            logger.error(f"Failed to process customer cancellation reasons: {str(e)}")
            failed_count += 1
    
    # Summary
    logger.info(f"""
    ========== ETL COMPLETE ==========
    Total records fetched: {total_fetched}
    Total records loaded: {total_loaded}
    Failed tasks: {failed_count}
    ==================================
    """)
    
    if failed_count > 0:
        raise RuntimeError(f"{failed_count} tasks failed. Check logs for details.")
    
    # Transform raw data to staging tables
    logger.info("Starting transformation to staging tables...")
    try:
        from transform_to_analytics_flow import transform_raw_to_staging
        # Run incremental transformation after successful ETL
        transform_raw_to_staging(incremental=True, run_quality_checks=True)
        logger.info("Staging transformation completed successfully")
    except Exception as e:
        logger.error(f"Staging transformation failed: {str(e)}")
        # Don't fail the entire flow if transformation fails
        # This allows raw data to be loaded even if transformation has issues


if __name__ == "__main__":
    # Example: Run for all offices, all entities, last 24 hours
    run_fieldroutes_etl()