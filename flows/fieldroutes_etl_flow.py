from __future__ import annotations
import pytz
"""
FieldRoutes → Snowflake ETL with enhanced Prefect patterns
• Uses Prefect's native retry mechanisms
• Better error handling and logging
• Supports both full and incremental extraction
"""

import json
import datetime
import urllib.parse
from typing import Dict, List, Optional, Tuple
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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

# Custom exceptions for better error handling
class FieldRoutesAPIError(Exception):
    """Raised when FieldRoutes API returns an error"""
    pass

class FieldRoutesRateLimitError(Exception):
    """Raised when hitting rate limits"""
    pass

# Helper functions
def chunk_list(items: List, chunk_size: int = 1000):
    """Split a list into chunks of specified size."""
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]

def is_retriable_error(exception):
    """Determine if an error should trigger a retry."""
    if isinstance(exception, requests.HTTPError):
        return exception.response.status_code >= 500
    return isinstance(exception, (requests.ConnectionError, requests.Timeout))

# =============================================================================
# Enhanced API interaction with native Prefect retries
# =============================================================================
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout, FieldRoutesRateLimitError))
)
def make_api_request(url: str, headers: Dict, params: Dict = None, timeout: int = 30) -> Dict:
    """Make API request with automatic retry logic."""
    response = requests.get(url, headers=headers, params=params, timeout=timeout)
    
    if response.status_code == 429:
        raise FieldRoutesRateLimitError("Rate limit exceeded")
    elif response.status_code >= 500:
        raise FieldRoutesAPIError(f"Server error: {response.status_code}")
    
    response.raise_for_status()
    return response.json()

# =============================================================================
# Main ETL Task
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
    """Enhanced fetch with timezone fix and dual date field support

    Fetch entity data from FieldRoutes API and load to Snowflake.
    
    Returns:
        Tuple of (records_fetched, records_loaded)
    """
    logger = get_run_logger()
    records = []
    unresolved_ids = []
    
    entity = meta["endpoint"]
    table_name = meta["table"]
    date_field = meta["date_field"]
    is_small_dim = meta["small"]
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
        "includeData": 1 if (is_small_dim and not window_start) else 0,
    }
    
    # Add date filter for incremental loads (fact tables only)
    if window_start and not is_dimension:
        # Convert UTC to Pacific Time
        pacific_tz = pytz.timezone('America/Los_Angeles')
        pt_start = window_start.astimezone(pacific_tz)
        pt_end = window_end.astimezone(pacific_tz)
        
        start_date = pt_start.strftime("%Y-%m-%d")
        end_date = pt_end.strftime("%Y-%m-%d")
        
        # Handle dual date field logic for dateUpdated entities
        if date_field == "dateUpdated":
            logger.info(f"Using dual date strategy for {entity}: searching both dateUpdated and dateAdded")
            
            # Strategy: Make two separate searches and combine results
            records_updated = []
            records_created = []
            
            # Search 1: Records updated in time window
            params_updated = params.copy()
            params_updated["dateUpdated"] = json.dumps({
                "operator": "BETWEEN",
                "value": [start_date, end_date]
            })
            
            try:
                search_url = f"{base_url}/{entity}/search"
                logger.info(f"Search 1: Records updated between {start_date} and {end_date}")
                search_data_updated = make_api_request(search_url, headers, params_updated)
                records_updated = search_data_updated.get("resolvedObjects", []) or search_data_updated.get("ResolvedObjects", [])
                unresolved_updated = search_data_updated.get(f"{entity}IDsNoDataExported", [])
                logger.info(f"Found {len(records_updated)} updated records, {len(unresolved_updated)} IDs to fetch")
            except Exception as e:
                logger.warning(f"Updated records search failed: {e}")
            
            # Search 2: Records created in time window with no updates (dateUpdated IS NULL)
            params_created = params.copy() 
            params_created["dateAdded"] = json.dumps({
                "operator": "BETWEEN",
                "value": [start_date, end_date]
            })
            # Note: PestRoutes API might not support IS NULL filters easily
            # So we'll get all records created in the time window and filter client-side
            unresolved_ids = []
            try:
                logger.info(f"Search 2: Records created between {start_date} and {end_date}")
                search_data_created = make_api_request(search_url, headers, params_created)
                all_created = search_data_created.get("resolvedObjects", []) or search_data_created.get("ResolvedObjects", [])
                # Filter to only records where dateUpdated is null or empty
                records_created = [r for r in all_created if not r.get("dateUpdated")]
                unresolved_created = search_data_created.get(f"{entity}IDsNoDataExported", [])
                logger.info(f"Found {len(all_created)} created records, {len(records_created)} with null dateUpdated")
            except Exception as e:
                logger.warning(f"Created records search failed: {e}")
            
            # Combine results and remove duplicates (by ID)
            all_records = records_updated + records_created
            all_unresolved = list(set(unresolved_updated + unresolved_created))  # Remove duplicate IDs
            
            # Remove duplicate records (in case a record appears in both searches)
            seen_ids = set()
            unique_records = []
            for record in all_records:
                record_id = record.get(f"{entity}ID") or record.get("id")
                if record_id not in seen_ids:
                    seen_ids.add(record_id)
                    unique_records.append(record)
            
            records = unique_records
            unresolved_ids = all_unresolved
            
        else:
            # Simple single date field search (for dateAdded entities)
            params[date_field] = json.dumps({
                "operator": "BETWEEN", 
                "value": [start_date, end_date]
            })
            
            search_url = f"{base_url}/{entity}/search"
            search_data = make_api_request(search_url, headers, params)
            records = search_data.get("resolvedObjects", []) or search_data.get("ResolvedObjects", [])
            unresolved_ids = search_data.get(f"{entity}IDsNoDataExported", [])
        
        logger.info(f"Total unique records after dual date search: {len(records)}")
        logger.info(f"Unresolved IDs to bulk fetch: {len(unresolved_ids)}")
    
    # == Step 2: Bulk fetch unresolved IDs ====================================
    for id_chunk in chunk_list(unresolved_ids, 1000):
        # Build query string for bulk fetch
        chunk_params = [(f"{entity}IDs", str(id)) for id in id_chunk]
        query_string = urllib.parse.urlencode(chunk_params, doseq=True)
        
        try:
            bulk_url = f"{base_url}/{entity}/get?{query_string}"
            bulk_data = make_api_request(bulk_url, headers, timeout=60)
            
            # Handle response format variations
            if isinstance(bulk_data, list):
                records.extend(bulk_data)
            else:
                records.extend(bulk_data.get("resolvedObjects", []))
                
        except Exception as e:
            logger.error(f"Bulk fetch failed for {entity} chunk: {str(e)}")
            # Continue with partial data rather than failing entirely
            continue
    
    total_records = len(records)
    logger.info(f"Total records fetched for {entity}: {total_records}")
    
    if total_records == 0:
        logger.warning(f"No records found for {entity} in office {office['office_id']}")
        return 0, 0
    
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
            
            for batch in chunk_list(records, batch_size):
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

# =============================================================================
# Monitoring task
# =============================================================================
@task(name="log_extraction_summary", tags=["monitoring"])
def log_extraction_summary(results: List[Tuple[str, int, int]]):
    """Log summary statistics for the extraction."""
    logger = get_run_logger()
    
    total_fetched = sum(r[1] for r in results)
    total_loaded = sum(r[2] for r in results)
    
    logger.info("="*60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("="*60)
    for entity, fetched, loaded in results:
        status = "done" if fetched == loaded else "warning"
        logger.info(f"{status} {entity}: Fetched={fetched}, Loaded={loaded}")
    logger.info(f"TOTAL: Fetched={total_fetched}, Loaded={total_loaded}")
    logger.info("="*60)
    
    if total_fetched != total_loaded:
        logger.warning("Some records were not loaded successfully")

# =============================================================================
# Schema validation task (optional but recommended)
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
            existing_tables = {row[0] for row in cursor.fetchall()}
        
        missing_tables = required_tables - existing_tables
        if missing_tables:
            logger.error(f"Missing tables in Snowflake: {missing_tables}")
            return False
        
        logger.info("All required tables exist in Snowflake")
        return True
        
    except Exception as e:
        logger.error(f"Schema validation failed: {str(e)}")
        return False