from __future__ import annotations
"""
FieldRoutes → Snowflake ETL - Direct Load Version
Bypasses Azure Blob Storage and writes directly to Snowflake
"""
import json
import time
import datetime
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
ENTITY_META = [
    # Dimension tables (reference data)
    ("customer",       "Customer_Dim",        True,  False, "dateUpdated"),
    ("employee",       "Employee_Dim",        True,  False, "dateUpdated"),
    ("office",         "Office_Dim",          True,   True, "dateAdded"),
    ("region",         "Region_Dim",          True,   True, "dateAdded"),
    ("serviceType",    "ServiceType_Dim",     True,   True, "dateAdded"),
    ("customerSource", "CustomerSource_Dim",  True,   True, "dateAdded"),
    ("genericFlag",    "GenericFlag_Dim",     True,   True, "dateAdded"),

    # Fact tables (transactional data)
    ("appointment",    "Appointment_Fact",    False, False, "dateUpdated"),
    ("subscription",   "Subscription_Fact",   False, False, "dateUpdated"),
    ("route",          "Route_Fact",          False, False, "dateUpdated"),
    ("ticket",         "Ticket_Dim",          False, False, "dateUpdated"),
    ("ticketItem",     "TicketItem_Fact",     False, False, "dateUpdated"),
    ("payment",        "Payment_Fact",        False, False, "dateUpdated"),
    ("appliedPayment", "AppliedPayment_Fact", False, False, "dateUpdated"),
    ("note",           "Note_Fact",           False, False, "dateAdded"),
    ("task",           "Task_Fact",           False, False, "dateAdded"),
    ("appointmentReminder", "AppointmentReminder_Fact", False, False, "dateUpdated"),
    ("door",           "DoorKnock_Fact",      False, False, "dateUpdated"),
    ("disbursement",   "FinancialTransaction_Fact", False, False, "dateUpdated"),
    ("chargeback",     "FinancialTransaction_Fact", False, False, "dateUpdated"),
    ("flagAssignment", "FlagAssignment_Fact", False, False, "dateAdded"),
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
    
    entity = meta["endpoint"]
    table_name = meta["table"]
    date_field = meta["date_field"]
    is_dimension = meta["is_dim"]
    
    logger.info(f"Starting fetch for {entity} - Office {office['office_id']} ({office['office_name']})")
    
    # API configuration
    base_url = office["base_url"]
    headers = {
        "AuthenticationKey": office["auth_key"],
        "AuthenticationToken": office["auth_token"],
    }
    
    # Build query parameters
    params = {
        "officeIDs": office["office_id"],
        "includeData": 0
    }
    
    # Convert UTC to Pacific Time for date filtering
    pacific_tz = pytz.timezone('America/Los_Angeles')
    pt_start = None
    pt_end = None
    
    # Convert dates if provided
    if window_start and window_end:
        pt_start = window_start.astimezone(pacific_tz)
        pt_end = window_end.astimezone(pacific_tz)
    
    # Add date filter for incremental loads (fact tables only)
    if window_start and window_end and not is_dimension:
        date_filter = {
            "operator": "BETWEEN",
            "value": [
                pt_start.strftime("%Y-%m-%d"),
                pt_end.strftime("%Y-%m-%d")
            ]
        }
        params[date_field] = json.dumps(date_filter)
        logger.info(f"Using date filter on {date_field}: {date_filter}")
    
    # == Step 1: Search for IDs ================================================
    search_url = f"{base_url}/{entity}/search"
    logger.info(f"Searching {entity} with params: {params}")
    
    all_ids = []
    
    try:
        search_data = make_api_request(search_url, headers, params)
        
        logger.info(f"Search response success: {search_data.get('success', False)}")
        logger.info(f"Count: {search_data.get('count', 0)}")
        
        # The propertyName field tells us where the IDs are
        if "propertyName" in search_data:
            id_field = search_data["propertyName"]
            logger.info(f"IDs are in field: {id_field}")
            
            if id_field in search_data:
                all_ids = search_data[id_field]
                logger.info(f"Found {len(all_ids)} IDs in {id_field}")
        else:
            # Fallback - try common patterns
            possible_fields = [
                f"{entity}IDs",
                f"{entity}IDsNoDataExported",
                "ids"
            ]
            for field in possible_fields:
                if field in search_data and search_data[field]:
                    all_ids = search_data[field]
                    logger.info(f"Found {len(all_ids)} IDs in {field}")
                    break
        
        # For dateUpdated entities, also search by dateAdded (only if we have date windows)
        if date_field == "dateUpdated" and pt_start and pt_end:
            logger.info(f"Performing additional search on dateAdded for {entity}")
            
            params_created = params.copy()
            params_created.pop("dateUpdated", None)
            params_created["dateAdded"] = json.dumps({
                "operator": "BETWEEN",
                "value": [
                    pt_start.strftime("%Y-%m-%d"),
                    pt_end.strftime("%Y-%m-%d")
                ]
            })
            
            try:
                created_data = make_api_request(search_url, headers, params_created)
                
                if "propertyName" in created_data and created_data["propertyName"] in created_data:
                    id_field = created_data["propertyName"]
                    created_ids = created_data[id_field]
                    logger.info(f"Found {len(created_ids)} additional IDs from dateAdded search")
                    
                    # Combine and deduplicate
                    all_ids = list(set(all_ids + created_ids))
                    
            except Exception as e:
                logger.warning(f"Failed to search by dateAdded: {e}")
        
    except Exception as e:
        logger.error(f"Search failed for {entity}: {str(e)}")
        raise
    
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
                "customer": "customers",
                "employee": "employees",
                "office": "offices",
                "region": "regions",
                "serviceType": "serviceTypes",
                "customerSource": "customerSources",
                "genericFlag": "genericFlags",
                "appointment": "appointments",
                "subscription": "subscriptions",
                "route": "routes",
                "ticket": "tickets",
                "ticketItem": "ticketItems",
                "payment": "payments",
                "appliedPayment": "appliedPayments",
                "note": "notes",
                "task": "tasks",
                "appointmentReminder": "appointmentReminders",
                "door": "doors",
                "disbursement": "disbursements",
                "chargeback": "chargebacks",
                "flagAssignment": "flagAssignments"
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
    load_timestamp = window_end.strftime("%Y-%m-%d %H:%M:%S") if window_end else datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    try:
        sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
        
        # Prepare data for bulk insert
        data_rows = []
        for record in all_records:
            # For financial transactions, add the transaction type
            if entity in ["disbursement", "chargeback"]:
                json_data = orjson.dumps(record).decode('utf-8') if HAS_ORJSON else json.dumps(record)
                data_rows.append((
                    office["office_id"],
                    load_timestamp,
                    json_data,
                    entity  # transaction_type
                ))
            else:
                json_data = orjson.dumps(record).decode('utf-8') if HAS_ORJSON else json.dumps(record)
                data_rows.append((
                    office["office_id"],
                    load_timestamp,
                    json_data
                ))
        
        # Bulk insert using Snowflake's executemany
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            
            # Clear any existing data for this office/entity/timestamp to prevent duplicates
            logger.info(f"Clearing existing data for office {office['office_id']} at timestamp {load_timestamp}")
            cursor.execute(f"""
                DELETE FROM RAW.fieldroutes.{table_name}
                WHERE OfficeID = %s AND LoadDatetimeUTC = %s
            """, (office["office_id"], load_timestamp))
            
            # Bulk insert new data
            logger.info(f"Inserting {len(data_rows)} records into {table_name}")
            
            if entity in ["disbursement", "chargeback"]:
                # Special handling for financial transactions
                cursor.executemany(f"""
                    INSERT INTO RAW.fieldroutes.{table_name} 
                    (OfficeID, LoadDatetimeUTC, RawData, TransactionType)
                    VALUES (%s, %s, PARSE_JSON(%s), %s)
                """, data_rows)
            else:
                cursor.executemany(f"""
                    INSERT INTO RAW.fieldroutes.{table_name} 
                    (OfficeID, LoadDatetimeUTC, RawData)
                    VALUES (%s, %s, PARSE_JSON(%s))
                """, data_rows)
            
            # Update watermark
            cursor.execute("""
                UPDATE RAW.REF.office_entity_watermark 
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
                    UPDATE RAW.REF.office_entity_watermark 
                    SET error_count = error_count + 1,
                        last_run_utc = %s
                    WHERE office_id = %s AND entity_name = %s
                """, (load_timestamp, office["office_id"], entity))
                conn.commit()
        except:
            pass  # Don't fail the whole task if we can't update the error count
            
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
                FROM RAW.REF.offices_lookup
                WHERE active = TRUE AND office_id IN ({placeholders})
            """
            cursor.execute(query, office_filter)
        else:
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW.REF.offices_lookup
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
    entities_to_process = [
        {"endpoint": ep, "table": tbl, "is_dim": dim, "small": small, "date_field": df}
        for ep, tbl, dim, small, df in ENTITY_META
        if not entity_filter or ep in entity_filter
    ]
    
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
                logger.error(f"❌ {meta['endpoint']} failed for office {office['office_id']}: {str(e)}")
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


if __name__ == "__main__":
    # Example: Run for all offices, all entities, last 24 hours
    run_fieldroutes_etl()