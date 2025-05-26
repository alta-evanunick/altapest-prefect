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
import pytz

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
def make_api_request(
    url: str, 
    headers: Dict, 
    params: Dict = None, 
    max_retries: int = 5,
    timeout: int = 30
) -> requests.Response:
    """Make API request with exponential backoff retry and proper encoding"""
    logger = get_run_logger()
    
    # Handle complex parameters that need special encoding
    if params and any(isinstance(v, str) and v.startswith('{') for v in params.values()):
        # Build URL manually for complex JSON parameters
        base_url = url.split('?')[0]
        query_parts = []
        
        for key, value in params.items():
            if isinstance(value, str) and value.startswith('{'):
                # This is a JSON parameter - needs special handling
                encoded_value = urllib.parse.quote(value)
                query_parts.append(f"{key}={encoded_value}")
            else:
                query_parts.append(f"{key}={value}")
        
        query_string = "&".join(query_parts)
        full_url = f"{base_url}?{query_string}"
        
        logger.debug(f"Manually constructed URL: {full_url}")
        
        # Make request without params (URL already has them)
        for attempt in range(max_retries):
            try:
                response = requests.get(full_url, headers=headers, timeout=timeout)
                
                # Log response for debugging
                logger.debug(f"Response status: {response.status_code}")
                logger.debug(f"Response headers: {dict(response.headers)}")
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    time.sleep(retry_after)
                    continue
                    
                response.raise_for_status()
                return response
                
            except requests.HTTPError as e:
                logger.error(f"HTTP Error: {e}")
                logger.error(f"Response body: {e.response.text if e.response else 'No response'}")
                if e.response and e.response.status_code >= 500:
                    # Server error - retry with backoff
                    time.sleep(2 ** attempt)
                    continue
                raise
            except (requests.ConnectionError, requests.Timeout):
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
    else:
        # Simple parameters - let requests handle it
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=timeout)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    time.sleep(retry_after)
                    continue
                    
                response.raise_for_status()
                return response
                
            except requests.HTTPError as e:
                if e.response and e.response.status_code >= 500:
                    # Server error - retry with backoff
                    time.sleep(2 ** attempt)
                    continue
                raise
            except (requests.ConnectionError, requests.Timeout):
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
    
    raise RuntimeError(f"Failed after {max_retries} attempts")

# =============================================================================
# Main ETL Task
# =============================================================================
@task(retries=2, retry_delay_seconds=60)
def fetch_entity_optimized(
    office: Dict,
    entity_config: EntityConfig,
    window_start: Optional[datetime.datetime] = None,
    window_end: Optional[datetime.datetime] = None,
    api_tracker: APICallTracker = None,
    mode: str = "full"  # "full" or "cdc"
) -> Tuple[int, int]:  # Returns (record_count, api_calls)
    """
    Optimized entity fetch with smart API usage
    """
    logger = get_run_logger()
    
    office_id = office["office_id"]
    entity = entity_config.endpoint
    
    logger.info(f"Starting fetch for {entity} - Office {office_id} ({office['office_name']})")
    
    # Check API budget
    if api_tracker and not api_tracker.can_make_call(office_id, 2):
        logger.warning(f"Skipping {entity} for office {office_id} - API budget exceeded")
        return 0, 0
    
    base_url = office["base_url"]
    headers = {
        "AuthenticationKey": office["auth_key"],
        "AuthenticationToken": office["auth_token"],
    }
    
    # Build query parameters - NOTE: officeIDs not officeID!
    params = {"officeIDs": office_id}
    
    # Convert UTC to Pacific Time if we have date windows
    pacific_tz = pytz.timezone('America/Los_Angeles')
    
    # Apply date filtering for CDC mode or incremental updates
    if window_start and window_end:
        # Convert to Pacific Time
        pt_start = window_start.astimezone(pacific_tz)
        pt_end = window_end.astimezone(pacific_tz)
        
        # For CDC, filter even dimension tables if they have dateUpdated
        if mode == "cdc" or not entity_config.is_dimension:
            # FieldRoutes expects date-only format in PT
            date_filter = {
                "operator": "BETWEEN",
                "value": [
                    pt_start.strftime("%Y-%m-%d"),
                    pt_end.strftime("%Y-%m-%d")
                ]
            }
            params[entity_config.date_field] = json.dumps(date_filter)
            logger.info(f"Using date filter on {entity_config.date_field}: {date_filter}")
    
    # Always use includeData=0 to get IDs first
    params["includeData"] = 0
    
    logger.debug(f"Search parameters: {params}")
    
    # Make search request
    api_calls = 1
    search_url = f"{base_url}/{entity}/search"
    
    try:
        search_response = make_api_request(search_url, headers=headers, params=params)
        
        if api_tracker:
            api_tracker.record_call(office_id, 1)
        
        search_data = search_response.json()
        logger.debug(f"Search response keys: {list(search_data.keys())}")
        
        # Log the count and propertyName
        if "count" in search_data:
            logger.info(f"Search returned count: {search_data['count']}")
        if "propertyName" in search_data:
            logger.info(f"IDs are in field: {search_data['propertyName']}")
        
        # Initialize collections
        all_records = []
        all_ids = []
        
        # Extract IDs from the response
        # The propertyName field tells us which field has the IDs
        if "propertyName" in search_data and search_data["propertyName"] in search_data:
            id_field = search_data["propertyName"]
            all_ids = search_data[id_field]
            logger.info(f"Found {len(all_ids)} IDs in {id_field}")
        else:
            # Fallback: try common field names
            possible_id_fields = [
                f"{entity}IDs",  # e.g., customerIDs
                f"{entity}IDsNoDataExported",  # When includeData=1 hits limit
                "ids",
                "IDs"
            ]
            
            for field in possible_id_fields:
                if field in search_data and isinstance(search_data[field], list):
                    all_ids = search_data[field]
                    logger.info(f"Found {len(all_ids)} IDs in {field}")
                    break
        
        # For entities with dateUpdated, also search by dateAdded
        if entity_config.date_field == "dateUpdated" and window_start and window_end:
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
                created_response = make_api_request(search_url, headers=headers, params=params_created)
                api_calls += 1
                if api_tracker:
                    api_tracker.record_call(office_id, 1)
                
                created_data = created_response.json()
                
                # Extract additional IDs
                created_ids = []
                if "propertyName" in created_data and created_data["propertyName"] in created_data:
                    id_field = created_data["propertyName"]
                    created_ids = created_data[id_field]
                    logger.info(f"Found {len(created_ids)} additional IDs from dateAdded search")
                
                # Combine and deduplicate IDs
                if created_ids:
                    all_ids = list(set(all_ids + created_ids))
                    logger.info(f"Total unique IDs after combining: {len(all_ids)}")
                    
            except Exception as e:
                logger.warning(f"Failed to search by dateAdded: {e}")
        
        # Now fetch the actual data using the get endpoint
        if all_ids:
            logger.info(f"Fetching data for {len(all_ids)} IDs using /get endpoint")
            
            # Check API budget for get calls
            get_calls_needed = (len(all_ids) + 999) // 1000
            if api_tracker and not api_tracker.can_make_call(office_id, get_calls_needed):
                logger.warning(f"Insufficient API budget for {get_calls_needed} get calls")
                return len(all_records), api_calls
            
            # Batch get calls (max 1000 IDs per call)
            for id_chunk in chunk_list(all_ids, 1000):
                # Build query string for batch get
                chunk_params = [(f"{entity}IDs", str(id)) for id in id_chunk]
                query_string = urllib.parse.urlencode(chunk_params, doseq=True)
                
                get_url = f"{base_url}/{entity}/get?{query_string}"
                logger.debug(f"Fetching chunk of {len(id_chunk)} records")
                
                try:
                    get_response = make_api_request(get_url, headers=headers)
                    api_calls += 1
                    if api_tracker:
                        api_tracker.record_call(office_id, 1)
                    
                    get_data = get_response.json()
                    
                    # The response structure varies by entity
                    # Check for data in various possible locations
                    chunk_records = []
                    
                    # Try the propertyNameData field first (if it exists)
                    if "propertyNameData" in get_data:
                        property_data = get_data["propertyNameData"]
                        if isinstance(property_data, dict):
                            # Data might be keyed by entity plural name
                            for key, value in property_data.items():
                                if isinstance(value, list):
                                    chunk_records = value
                                    break
                        elif isinstance(property_data, list):
                            chunk_records = property_data
                    
                    # Fallback to other common patterns
                    if not chunk_records:
                        # Try plural entity name
                        plural_map = {
                            "customer": "customers",
                            "employee": "employees",
                            "appointment": "appointments",
                            "payment": "payments",
                            # Add more as needed
                        }
                        plural_name = plural_map.get(entity, entity + "s")
                        
                        if plural_name in get_data:
                            chunk_records = get_data[plural_name]
                        elif isinstance(get_data, list):
                            chunk_records = get_data
                    
                    if chunk_records:
                        all_records.extend(chunk_records)
                        logger.debug(f"Retrieved {len(chunk_records)} records in this chunk")
                    else:
                        logger.warning(f"No records found in get response. Keys: {list(get_data.keys())}")
                    
                except Exception as e:
                    logger.error(f"Failed to fetch chunk: {e}")
                    continue
        else:
            logger.warning(f"No IDs found for {entity} in office {office_id}")
        
        logger.info(f"Total records fetched: {len(all_records)} using {api_calls} API calls")
        
        # Load to Snowflake if we have records
        if all_records:
            load_timestamp = window_end.strftime("%Y-%m-%d %H:%M:%S") if window_end else datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
            
            sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
            with sf_connector.get_connection() as conn:
                cursor = conn.cursor()
                
                # Prepare data for insertion
                insert_data = [
                    (office_id, load_timestamp, json.dumps(record))
                    for record in all_records
                ]
                
                # Batch insert
                cursor.executemany(
                    f"INSERT INTO RAW.fieldroutes.{entity_config.table_name} (OfficeID, LoadDatetimeUTC, RawData) "
                    f"VALUES (%s, %s, PARSE_JSON(%s))",
                    insert_data
                )
                
                # Update watermark
                cursor.execute(
                    "MERGE INTO RAW.REF.office_entity_watermark AS target "
                    "USING (SELECT %s AS office_id, %s AS entity_name, %s AS last_run_utc, %s AS records_loaded) AS source "
                    "ON target.office_id = source.office_id AND target.entity_name = source.entity_name "
                    "WHEN MATCHED THEN UPDATE SET last_run_utc = source.last_run_utc, records_loaded = source.records_loaded, last_success_utc = CURRENT_TIMESTAMP() "
                    "WHEN NOT MATCHED THEN INSERT (office_id, entity_name, last_run_utc, records_loaded) "
                    "VALUES (source.office_id, source.entity_name, source.last_run_utc, source.records_loaded)",
                    (office_id, entity, load_timestamp, len(all_records))
                )
                
                conn.commit()
                logger.info(f"Successfully loaded {len(all_records)} records to Snowflake")
        
        return len(all_records), api_calls
        
    except Exception as e:
        logger.error(f"Failed to fetch {entity} for office {office_id}: {str(e)}")
        raise

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