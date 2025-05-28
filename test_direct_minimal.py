#!/usr/bin/env python3
"""
Minimal test script for FieldRoutes API to Snowflake direct load
Tests customer entity for office 1 only
"""
import sys
import os
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_customer_office1():
    """Test loading customer entity from office 1"""
    
    # Import dependencies
    try:
        import pytz
        import requests
        from prefect.blocks.system import Secret
        from prefect_snowflake import SnowflakeConnector
    except ImportError as e:
        logger.error(f"Missing dependency: {e}")
        return False
    
    # Check if we have json fallback
    try:
        import orjson
        logger.info("Using orjson for JSON serialization")
        json_dumps = lambda x: orjson.dumps(x).decode('utf-8')
    except ImportError:
        import json
        logger.info("Using standard json module")
        json_dumps = json.dumps
    
    logger.info("Testing customer entity for office_id=1")
    
    # Get office configuration
    try:
        sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
        with sf_connector.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW.REF.offices_lookup
                WHERE office_id = 1 AND active = TRUE
            """)
            
            row = cursor.fetchone()
            if not row:
                logger.error("Office 1 not found or not active!")
                return False
                
            office_id, name, base_url, key_block, token_block = row
            
        logger.info(f"Found office: {office_id} - {name}")
        logger.info(f"Base URL: {base_url}")
        
        # Get credentials
        auth_key = Secret.load(key_block).get()
        auth_token = Secret.load(token_block).get()
        
        headers = {
            "AuthenticationKey": auth_key,
            "AuthenticationToken": auth_token,
        }
        
        # Test with a small time window (last 7 days)
        pacific_tz = pytz.timezone('America/Los_Angeles')
        now = datetime.now(pacific_tz)
        window_start = now - timedelta(days=7)
        window_end = now
        
        # Build search parameters
        params = {
            "officeIDs": office_id,
            "includeData": 0
        }
        
        # Add date filter
        date_filter = {
            "operator": "BETWEEN",
            "value": [
                window_start.strftime("%Y-%m-%d"),
                window_end.strftime("%Y-%m-%d")
            ]
        }
        params["dateUpdated"] = json_dumps(date_filter)
        
        # Search for customer IDs
        search_url = f"{base_url}/customer/search"
        logger.info(f"Searching for customers updated in last 7 days...")
        logger.info(f"URL: {search_url}")
        logger.info(f"Params: {params}")
        
        response = requests.get(search_url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"Search failed with status {response.status_code}: {response.text}")
            return False
            
        search_data = response.json()
        logger.info(f"Search response success: {search_data.get('success', False)}")
        logger.info(f"Count: {search_data.get('count', 0)}")
        
        # Find IDs in response
        customer_ids = []
        if "propertyName" in search_data and search_data["propertyName"] in search_data:
            id_field = search_data["propertyName"]
            customer_ids = search_data[id_field]
            logger.info(f"Found {len(customer_ids)} customer IDs in field '{id_field}'")
        else:
            # Try common patterns
            for field in ["customerIDs", "customerIDsNoDataExported", "ids"]:
                if field in search_data:
                    customer_ids = search_data[field]
                    logger.info(f"Found {len(customer_ids)} customer IDs in field '{field}'")
                    break
        
        if not customer_ids:
            logger.warning("No customer IDs found for the date range")
            return True  # Not a failure, just no data
        
        # Fetch first 10 customers as a test
        test_ids = customer_ids[:10]
        logger.info(f"Fetching details for {len(test_ids)} customers (out of {len(customer_ids)} total)")
        
        # Build GET URL
        id_list = ",".join(str(id) for id in test_ids)
        get_url = f"{base_url}/customer/get?customerIDs=[{id_list}]"
        
        response = requests.get(get_url, headers=headers, timeout=60)
        
        if response.status_code != 200:
            logger.error(f"Get failed with status {response.status_code}: {response.text}")
            return False
            
        get_data = response.json()
        
        # Find customer records
        customers = []
        if "customers" in get_data:
            customers = get_data["customers"]
        elif isinstance(get_data, list):
            customers = get_data
        
        logger.info(f"Retrieved {len(customers)} customer records")
        
        if customers:
            # Show sample customer
            sample = customers[0]
            logger.info(f"Sample customer: ID={sample.get('customerID')}, Name={sample.get('firstName')} {sample.get('lastName')}")
        
        # Test Snowflake insert with one record
        if customers:
            logger.info("Testing Snowflake insert with 1 record...")
            
            load_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            test_record = customers[0]
            
            with sf_connector.get_connection() as conn:
                cursor = conn.cursor()
                
                # Test insert
                cursor.execute("""
                    INSERT INTO RAW.fieldroutes.Customer_Dim 
                    (OfficeID, LoadDatetimeUTC, RawData)
                    SELECT %s, %s, PARSE_JSON(%s)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM RAW.fieldroutes.Customer_Dim 
                        WHERE OfficeID = %s 
                        AND LoadDatetimeUTC = %s
                        AND RawData:customerID = %s
                    )
                """, (
                    office_id, 
                    load_timestamp, 
                    json_dumps(test_record),
                    office_id,
                    load_timestamp,
                    test_record.get('customerID')
                ))
                
                rows_inserted = cursor.rowcount
                conn.commit()
                
                logger.info(f"Snowflake insert test: {rows_inserted} rows inserted")
                
                # Verify the insert
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM RAW.fieldroutes.Customer_Dim 
                    WHERE OfficeID = %s 
                    AND _loaded_at >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
                """, (office_id,))
                
                count = cursor.fetchone()[0]
                logger.info(f"Verification: {count} recent records in Snowflake for office {office_id}")
        
        logger.info("✅ Test completed successfully!")
        logger.info(f"Summary: Found {len(customer_ids)} customers, fetched {len(customers)}, tested Snowflake insert")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
        return False


if __name__ == "__main__":
    success = test_customer_office1()
    sys.exit(0 if success else 1)