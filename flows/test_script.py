import requests
import json
from datetime import datetime, timedelta
import pytz

def test_customer_api():
    base_url = "https://alta.pestroutes.com/api"
    headers = {
        "authenticationKey": "7q23r7k2ta6r4s5rfim4v61p23r40jgmchkokr7mditft3r6mbbqa1df3bjcf49m",
        "authenticationToken": "784onqelmht4hrt9i2p15novjna792d8btfjevgmassojvdao0f1d1akueifc3dj"
    }
    
    # Test search with includeData=0
    params = {
        "officeIDs": 1,  # Note: plural!
        "includeData": 0,
        "dateUpdated": json.dumps({
            "operator": "BETWEEN",
            "value": ["2025-05-24", "2025-05-25"]
        })
    }
    
    response = requests.get(f"{base_url}/customer/search", headers=headers, params=params)
    data = response.json()
    
    print(f"Count: {data.get('count', 0)}")
    print(f"Property name: {data.get('propertyName', 'N/A')}")
    
    # Get the IDs
    id_field = data.get('propertyName', 'customerIDs')
    ids = data.get(id_field, [])
    print(f"Found {len(ids)} IDs")
    
    # Test get with first 5 IDs
    if ids:
        test_ids = ids[:5]
        get_params = [(f"customerIDs", str(id)) for id in test_ids]
        query_string = "&".join([f"{k}={v}" for k, v in get_params])
        
        get_response = requests.get(f"{base_url}/customer/get?{query_string}", headers=headers)
        get_data = get_response.json()
        
        print(f"\nGet response keys: {list(get_data.keys())}")
        
        # Check for customer data
        if "customers" in get_data:
            print(f"Found {len(get_data['customers'])} customers")
            if get_data['customers']:
                print(f"First customer: {get_data['customers'][0].get('firstName', '')} {get_data['customers'][0].get('lastName', '')}")

if __name__ == "__main__":
    test_customer_api()