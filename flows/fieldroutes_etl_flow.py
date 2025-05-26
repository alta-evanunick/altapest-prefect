import json
import time
import datetime
import urllib.parse
from typing import Dict, List, Optional, Tuple
import requests
import pytz
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
# Main ETL Task - CORRECTED VERSION
# =============================================================================
@task(
    name="fetch_entity",
    description="Fetch data for one entity from one office",
    retries=2,
    retry_delay_seconds=60,
    tags=["api", "extract"]
)
def fetch_entity(
    office: Dict, 
    meta: Dict, 
    window_start: Optional[datetime.datetime] = None, 
    window_end: Optional[datetime.datetime] = None
) -> Tuple[int, int]:
    """
    Fetch entity data from FieldRoutes API and load to Snowflake.
    
    Returns:
        Tuple of (records_fetched, records_loaded)
    """
    logger = get_run_logger()
    
    entity = meta["endpoint"]
    table_name = meta["table"]
    date_field = meta["date_field"]
    is_small_dim = meta["small"]
    is_dimension = meta["is_dim"]
    
    logger.info(f"Starting fetch for {entity} - Office {office['office_id']} ({office['office_name']})")
    
    # API configuration
    base_url = office["base_url"]
    headers = {
        "AuthenticationKey": office["auth_key"],  # Note: capital letters!
        "AuthenticationToken": office["auth_token"],
    }
    
    # Build query parameters - Use officeIDs (plural!)
    params = {
        "officeIDs": office["office_id"],
        "includeData": 0  # Always use 0 based on API behavior
    }
    
    # Convert UTC to Pacific Time for date filtering
    pacific_tz = pytz.timezone('America/Los_Angeles')
    
    # Add date filter for incremental loads (fact tables only)
    if window_start and not is_dimension:
        # Convert to Pacific Time
        pt_start = window_start.astimezone(pacific_tz)
        pt_end = window_end.astimezone(pacific_tz)
        
        # FieldRoutes expects date-only format
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
        
        # Log what we got back
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
        
        # For dateUpdated entities, also search by dateAdded
        if date_field == "dateUpdated" and window_start:
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
        chunk_params = [(f"{entity}IDs", str(id)) for id in id_chunk]
        query_string = urllib.parse.urlencode(chunk_params, doseq=True)
        
        get_url = f"{base_url}/{entity}/get?{query_string}"
        logger.info(f"Fetching chunk of {len(id_chunk)} records")
        
        try:
            get_data = make_api_request(get_url, headers, timeout=60)
            
            # The response structure varies by entity
            # Try different locations where data might be
            chunk_records = []
            
            # First try the plural entity name (most common)
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
    
    # == Step 3: Load to Snowflake ============================================
    load_timestamp = window_end.strftime("%Y-%m-%d %H:%M:%S") if window_end else datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            
            # Batch insert records
            insert_query = f"""
                INSERT INTO RAW.fieldroutes.{table_name} 
                (OfficeID, LoadDatetimeUTC, RawData) 
                VALUES (%s, %s, PARSE_JSON(%s))
            """
            
            # Process in batches to avoid memory issues
            batch_size = 5000
            total_loaded = 0
            
            for batch in chunk_list(all_records, batch_size):
                batch_data = [
                    (office["office_id"], load_timestamp, json.dumps(record))
                    for record in batch
                ]
                cursor.executemany(insert_query, batch_data)
                total_loaded += len(batch)
                logger.info(f"Loaded batch of {len(batch)} records ({total_loaded}/{total_records})")
            
            # Update watermark
            cursor.execute("""
                UPDATE RAW.REF.office_entity_watermark 
                SET last_run_utc = %s, 
                    records_loaded = %s,
                    last_success_utc = CURRENT_TIMESTAMP()
                WHERE office_id = %s AND entity_name = %s
            """, (load_timestamp, total_loaded, office["office_id"], entity))
            
            conn.commit()
            logger.info(f"Successfully loaded {total_loaded} records for {entity}")

    except Exception as e:
        logger.error(f"Snowflake load failed for {entity}: {str(e)}")
        raise
    
    return total_records, total_loaded