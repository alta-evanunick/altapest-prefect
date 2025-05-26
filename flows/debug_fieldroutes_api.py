"""
Debug script to test FieldRoutes API connectivity and diagnose zero records issue
"""
import json
import requests
import urllib.parse
from datetime import datetime, timedelta
import pytz
from prefect.blocks.system import Secret
from prefect_snowflake import SnowflakeConnector

def debug_api_call(office_id: int = None):
    """Test API calls with different parameter combinations"""
    
    # Get office configuration
    sf_connector = SnowflakeConnector.load("snowflake-altapestdb")
    with sf_connector.get_connection() as conn:
        cursor = conn.cursor()
        
        if office_id:
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW.REF.offices_lookup
                WHERE office_id = %s AND active = TRUE
            """, (office_id,))
        else:
            cursor.execute("""
                SELECT office_id, office_name, base_url,
                       secret_block_name_key, secret_block_name_token
                FROM RAW.REF.offices_lookup
                WHERE active = TRUE
                LIMIT 1
            """)
        
        row = cursor.fetchone()
        if not row:
            print("No office found!")
            return
            
        office_id, name, base_url, key_block, token_block = row
    
    # Get credentials
    auth_key = Secret.load(key_block).get()
    auth_token = Secret.load(token_block).get()
    
    headers = {
        "AuthenticationKey": auth_key,
        "AuthenticationToken": auth_token
    }
    
    print(f"\nTesting office {office_id} - {name}")
    print(f"Base URL: {base_url}")
    
    # Test 1: Basic request without filters
    print("\n=== TEST 1: Basic customer search (no date filter) ===")
    params = {
        "officeIDs": office_id,
        "includeData": 1
    }
    
    response = requests.get(f"{base_url}/customer/search", headers=headers, params=params)
    print(f"Status: {response.status_code}")
    print(f"URL: {response.url}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Response keys: {list(data.keys())}")
        
        # Check different possible response formats
        record_count = 0
        if "resolvedObjects" in data:
            record_count = len(data["resolvedObjects"])
        elif "ResolvedObjects" in data:
            record_count = len(data["ResolvedObjects"])
        elif isinstance(data, list):
            record_count = len(data)
            
        print(f"Records returned: {record_count}")
        
        if "customerIDsNoDataExported" in data:
            print(f"Additional IDs beyond limit: {len(data['customerIDsNoDataExported'])}")
    else:
        print(f"Error response: {response.text}")
    
    # Test 2: With date filter
    print("\n=== TEST 2: Customer search with date filter ===")
    
    # Get date range in Pacific Time
    pacific_tz = pytz.timezone('America/Los_Angeles')
    now_pt = datetime.now(pacific_tz)
    yesterday_pt = now_pt - timedelta(days=1)
    
    date_filter = {
        "operator": "BETWEEN",
        "value": [
            yesterday_pt.strftime("%Y-%m-%d"),
            now_pt.strftime("%Y-%m-%d")
        ]
    }
    
    params = {
        "officeIDs": office_id,
        "includeData": 1,
        "dateUpdated": json.dumps(date_filter)
    }
    
    print(f"Date filter: {date_filter}")
    
    # Test both manual URL construction and requests library
    print("\n--- Using requests library automatic encoding ---")
    response = requests.get(f"{base_url}/customer/search", headers=headers, params=params)
    print(f"Status: {response.status_code}")
    print(f"URL: {response.url}")
    
    if response.status_code == 200:
        data = response.json()
        record_count = len(data.get("resolvedObjects", data.get("ResolvedObjects", [])))
        print(f"Records returned: {record_count}")
    else:
        print(f"Error: {response.text}")
    
    print("\n--- Using manual URL construction ---")
    query_parts = []
    for key, value in params.items():
        if key == "dateUpdated":
            encoded_value = urllib.parse.quote(value)
            query_parts.append(f"{key}={encoded_value}")
        else:
            query_parts.append(f"{key}={value}")
    
    query_string = "&".join(query_parts)
    manual_url = f"{base_url}/customer/search?{query_string}"
    
    print(f"Manual URL: {manual_url}")
    
    response = requests.get(manual_url, headers=headers)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        record_count = len(data.get("resolvedObjects", data.get("ResolvedObjects", [])))
        print(f"Records returned: {record_count}")
    else:
        print(f"Error: {response.text}")
    
    # Test 3: Try dateAdded instead
    print("\n=== TEST 3: Customer search with dateAdded filter ===")
    params["dateAdded"] = params.pop("dateUpdated")
    
    response = requests.get(f"{base_url}/customer/search", headers=headers, params=params)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        record_count = len(data.get("resolvedObjects", data.get("ResolvedObjects", [])))
        print(f"Records with dateAdded filter: {record_count}")
    else:
        print(f"Error: {response.text}")

if __name__ == "__main__":
    # Run debug for first office or specific office
    debug_api_call()  # Will use first active office
    # debug_api_call(office_id=123)  # Use specific office ID