"""
FieldRoutes Employee ETL Flow for Prefect
Handles incremental extraction from Seattle office Employee API
"""

import json
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import uuid

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeCredentials, SnowflakeConnector
from prefect.task_runners import SequentialTaskRunner


# Configuration
SEATTLE_OFFICE_ID = 1
SEATTLE_BASE_URL = "https://altapest.pestroutes.com/api"
BATCH_SIZE = 1000


@task(retries=3, retry_delay_seconds=30)
def get_office_config(office_id: int) -> Dict[str, Any]:
    """Retrieve office configuration from Snowflake"""
    logger = get_run_logger()
    
    snowflake_creds = SnowflakeCredentials.load("fieldroutes-snowflake")
    
    with SnowflakeConnector(credentials=snowflake_creds) as connector:
        result = connector.fetch_one(
            "SELECT * FROM office_config WHERE office_id = %s",
            parameters=(office_id,)
        )
    
    if not result:
        raise ValueError(f"Office {office_id} not found in configuration")
    
    logger.info(f"Retrieved config for office: {result[1]}")  # office_name
    return {
        'office_id': result[0],
        'office_name': result[1], 
        'base_url': result[2],
        'auth_key_secret_name': result[3],
        'auth_token_secret_name': result[4],
        'last_successful_run_utc': result[5]
    }


@task
def get_api_credentials(config: Dict[str, Any]) -> Dict[str, str]:
    """Retrieve API credentials from Prefect Secret blocks"""
    auth_key = Secret.load(config['auth_key_secret_name']).get()
    auth_token = Secret.load(config['auth_token_secret_name']).get()
    
    return {
        'authenticationKey': auth_key,
        'authenticationToken': auth_token
    }


@task(retries=3, retry_delay_seconds=30)
def search_employees(
    base_url: str,
    office_id: int, 
    credentials: Dict[str, str],
    date_updated_start: Optional[str] = None,
    date_updated_end: Optional[str] = None
) -> Dict[str, Any]:
    """Search for employee IDs using FieldRoutes API search endpoint"""
    logger = get_run_logger()
    
    search_url = f"{base_url}/employee/search"
    
    # Build search payload
    payload = {
        **credentials,
        'officeIDs': [office_id],
        'includeData': 0  # Don't include full data, just get IDs
    }
    
    # Add date filters for incremental loading
    if date_updated_start:
        payload['dateUpdatedStart'] = date_updated_start
    if date_updated_end:
        payload['dateUpdatedEnd'] = date_updated_end
    
    logger.info(f"Searching employees with payload keys: {list(payload.keys())}")
    
    response = requests.post(search_url, json=payload, timeout=60)
    response.raise_for_status()
    
    result = response.json()
    logger.info(f"Search returned {len(result.get('employeeIDsNoDataExported', []))} employee IDs")
    
    return result


@task(retries=3, retry_delay_seconds=30)
def get_employees_bulk(
    base_url: str,
    employee_ids: List[int],
    credentials: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Fetch employee details in bulk using FieldRoutes API get endpoint"""
    logger = get_run_logger()
    
    if not employee_ids:
        logger.info("No employee IDs to fetch")
        return []
    
    get_url = f"{base_url}/employee/get"
    
    # Chunk IDs into batches of 1000
    all_employees = []
    
    for i in range(0, len(employee_ids), BATCH_SIZE):
        chunk = employee_ids[i:i + BATCH_SIZE]
        
        payload = {
            **credentials,
            'employeeIDs': chunk
        }
        
        logger.info(f"Fetching batch {i//BATCH_SIZE + 1}: {len(chunk)} employees")
        
        response = requests.post(get_url, json=payload, timeout=120)
        response.raise_for_status()
        
        batch_result = response.json()
        employees = batch_result.get('employees', [])
        all_employees.extend(employees)
    
    logger.info(f"Retrieved {len(all_employees)} total employee records")
    return all_employees


@task
def load_raw_to_snowflake(
    employees: List[Dict[str, Any]], 
    office_id: int,
    batch_id: str
) -> int:
    """Load raw employee JSON data to Snowflake staging table"""
    logger = get_run_logger()
    
    if not employees:
        logger.info("No employees to load")
        return 0
    
    snowflake_creds = SnowflakeCredentials.load("fieldroutes-snowflake")
    
    # Prepare data for insertion
    insert_data = []
    for emp in employees:
        insert_data.append((
            office_id,
            emp.get('employeeID'),
            json.dumps(emp),
            batch_id
        ))
    
    with SnowflakeConnector(credentials=snowflake_creds) as connector:
        # Clear existing data for this batch (in case of retry)
        connector.execute(
            "DELETE FROM employee_raw WHERE office_id = %s AND batch_id = %s",
            parameters=(office_id, batch_id)
        )
        
        # Insert new data
        connector.execute_many(
            """INSERT INTO employee_raw (office_id, employee_id, raw_json, batch_id)
               VALUES (%s, %s, %s, %s)""",
            seq_of_parameters=insert_data
        )
    
    logger.info(f"Loaded {len(insert_data)} raw employee records")
    return len(insert_data)


@task
def transform_and_load_dimension(office_id: int, batch_id: str) -> int:
    """Transform raw JSON data and load into employee_dim table"""
    logger = get_run_logger()
    
    snowflake_creds = SnowflakeCredentials.load("fieldroutes-snowflake")
    
    with SnowflakeConnector(credentials=snowflake_creds) as connector:
        # Transform and insert using SQL
        result = connector.execute("""
            INSERT INTO employee_dim (
                employee_id, office_id, employee_type, active_flag,
                experience_years, supervisor_id, skill_list, team_list,
                first_name, last_name, email, phone, hire_date,
                effective_start_date, effective_end_date, is_current,
                source_system, load_datetime_utc, source_endpoint
            )
            SELECT 
                raw_json:employeeID::INT as employee_id,
                office_id,
                raw_json:type::VARCHAR as employee_type,
                raw_json:active::BOOLEAN as active_flag,
                raw_json:experience::VARCHAR as experience_years,
                raw_json:supervisorID::INT as supervisor_id,
                raw_json:skillList::VARCHAR as skill_list,
                raw_json:teamList::VARCHAR as team_list,
                raw_json:fname::VARCHAR as first_name,
                raw_json:lname::VARCHAR as last_name,
                raw_json:email::VARCHAR as email,
                raw_json:phone::VARCHAR as phone,
                raw_json:dateHired::DATE as hire_date,
                CURRENT_TIMESTAMP() as effective_start_date,
                NULL as effective_end_date,
                TRUE as is_current,
                'FieldRoutes' as source_system,
                CURRENT_TIMESTAMP() as load_datetime_utc,
                '/employee' as source_endpoint
            FROM employee_raw 
            WHERE office_id = %s AND batch_id = %s
        """, parameters=(office_id, batch_id))
        
        rows_inserted = result.rowcount if hasattr(result, 'rowcount') else 0
    
    logger.info(f"Transformed and loaded {rows_inserted} employee dimension records")
    return rows_inserted


@task
def update_office_config(office_id: int, run_end_time: str) -> None:
    """Update the last successful run timestamp in office config"""
    logger = get_run_logger()
    
    snowflake_creds = SnowflakeCredentials.load("fieldroutes-snowflake")
    
    with SnowflakeConnector(credentials=snowflake_creds) as connector:
        connector.execute(
            """UPDATE office_config 
               SET last_successful_run_utc = %s, updated_at = CURRENT_TIMESTAMP()
               WHERE office_id = %s""",
            parameters=(run_end_time, office_id)
        )
    
    logger.info(f"Updated last successful run time for office {office_id}")


@flow(
    name="fieldroutes-employee-etl",
    description="Extract employee data from FieldRoutes API and load to Snowflake",
    task_runner=SequentialTaskRunner(),
    log_prints=True
)
def fieldroutes_employee_etl(office_id: int = SEATTLE_OFFICE_ID):
    """Main ETL flow for FieldRoutes employee data"""
    logger = get_run_logger()
    
    # Generate unique batch ID for this run
    batch_id = f"emp_{office_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    run_start_time = datetime.now(timezone.utc)
    
    logger.info(f"Starting employee ETL for office {office_id}, batch: {batch_id}")
    
    try:
        # 1. Get office configuration
        config = get_office_config(office_id)
        
        # 2. Get API credentials
        credentials = get_api_credentials(config)
        
        # 3. Determine time window for incremental load
        date_updated_start = None
        if config['last_successful_run_utc']:
            date_updated_start = config['last_successful_run_utc'].strftime('%Y-%m-%d %H:%M:%S')
        
        date_updated_end = run_start_time.strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Loading employees updated between {date_updated_start} and {date_updated_end}")
        
        # 4. Search for employee IDs
        search_result = search_employees(
            config['base_url'],
            office_id,
            credentials,
            date_updated_start,
            date_updated_end
        )
        
        # 5. Get employee IDs that need full data fetch
        employee_ids = search_result.get('employeeIDsNoDataExported', [])
        
        # Also check if any employees were returned directly from search
        direct_employees = search_result.get('employees', [])
        
        # 6. Fetch full employee data
        if employee_ids:
            employees = get_employees_bulk(config['base_url'], employee_ids, credentials)
        else:
            employees = []
        
        # Combine with any direct employees from search
        employees.extend(direct_employees)
        
        if not employees:
            logger.info("No employees to process")
            return {"status": "success", "employees_processed": 0}
        
        # 7. Load raw data to Snowflake
        raw_count = load_raw_to_snowflake(employees, office_id, batch_id)
        
        # 8. Transform and load to dimension table
        dim_count = transform_and_load_dimension(office_id, batch_id)
        
        # 9. Update office config with successful run time
        update_office_config(office_id, run_start_time.isoformat())
        
        logger.info(f"ETL completed successfully: {dim_count} employees processed")
        
        return {
            "status": "success",
            "employees_processed": dim_count,
            "batch_id": batch_id,
            "run_time": run_start_time.isoformat()
        }
        
    except Exception as e:
        logger.error(f"ETL failed: {str(e)}")
        raise


if __name__ == "__main__":
    # For local testing
    fieldroutes_employee_etl()
