import requests
import pyodbc
import json

# --- CONFIGURATION ---
API_URL = "https://alta.conveyour.com/api/contacts?format=v2"
SQL_SERVER = "altaazureserver1.database.windows.net"
SQL_DATABASE = "AltaPestAnalytics"
SQL_USERNAME = "altapestadmin"
SQL_PASSWORD = "Altapest!"  # Replace with your actual password
SQL_DRIVER = "ODBC Driver 17 for SQL Server"  # Make sure this driver is installed
TABLE_NAME = "conveyour.contacts"
APP_KEY = "Oe4c6A5obvv00kDAU7KawSc80wrgWPDp"
TOKEN = "adzm2H8WKTLgtr5PNm3Bn3x5GHTtldb1"
HEADERS = {
    "x-conveyour-appkey": APP_KEY,
    "x-conveyour-token": TOKEN
}

# --- FETCH DATA FROM API ---
all_results = []

# First request to get total pages
response = requests.get(f"{API_URL}&page=1", headers=HEADERS)
response.raise_for_status()
data = response.json()

# Get pagination info
pagination = data.get('data', {})
total_pages = pagination.get('pages', 1)

print(f"Total pages to fetch: {total_pages}")

# Collect first page results
all_results.extend(pagination.get('results', []))

# Fetch remaining pages
for page in range(2, total_pages + 1):
    print(f"Fetching page {page} of {total_pages}")
    response = requests.get(f"{API_URL}&page={page}", headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    results = data.get('data', {}).get('results', [])
    all_results.extend(results)

if not all_results:
    print("No results found in API response.")
    exit(1)

results = all_results

# --- FLATTEN AND PREPARE FIELDS ---
# Use the first result to get all possible fields
sample = results[0]
fields = list(sample.keys())

# For arrays, we'll store as JSON strings
array_fields = [k for k, v in sample.items() if isinstance(v, list)]

# --- SQL TABLE CREATION ---
def sql_type(py_val):
    if isinstance(py_val, bool):
        return "BIT"
    elif isinstance(py_val, int):
        return "INT"
    elif isinstance(py_val, float):
        return "FLOAT"
    elif isinstance(py_val, list) or isinstance(py_val, dict):
        return "NVARCHAR(MAX)"
    else:
        return "NVARCHAR(255)"

def get_table_schema():
    schema = []
    for field in fields:
        val = sample[field]
        col_type = sql_type(val)
        null_str = "NOT NULL" if field == "con_id" else "NULL"
        pk_str = "PRIMARY KEY" if field == "con_id" else ""
        schema.append(f"[{field}] {col_type} {null_str} {pk_str}")
    return ", ".join(schema)

# --- CONNECT TO AZURE SQL ---
conn_str = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USERNAME};"
    f"PWD={SQL_PASSWORD}"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# --- CREATE TABLE IF NOT EXISTS ---
create_table_sql = f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'conveyour' AND TABLE_NAME = 'contacts')
BEGIN
    CREATE TABLE {TABLE_NAME} (
        {get_table_schema()}
    )
END
"""
cursor.execute(create_table_sql)
conn.commit()

# --- UPSERT DATA ---
for row in results:
    values = []
    for field in fields:
        val = row.get(field)
        if field in array_fields:
            val = json.dumps(val) if val is not None else None
        values.append(val)
    placeholders = ','.join(['?'] * len(fields))
    update_assignments = ', '.join([f"[{f}] = ?" for f in fields if f != 'con_id'])
    upsert_sql = f"""
    MERGE [conveyour].[contacts] AS target
    USING (SELECT ? AS [con_id]) AS source
    ON (target.[con_id] = source.[con_id])
    WHEN MATCHED THEN
        UPDATE SET {update_assignments}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join([f'[{f}]' for f in fields])})
        VALUES ({placeholders});
    """
    # For update, need to pass values for all fields except con_id
    update_values = [row.get(f) if f not in array_fields else json.dumps(row.get(f)) for f in fields if f != 'con_id']
    cursor.execute(upsert_sql, row['con_id'], *update_values, *values)
conn.commit()
cursor.close()
conn.close()

print("Data loaded successfully.")

# --- SCHEDULING INSTRUCTIONS ---
# To run this script daily, use Windows Task Scheduler or a cron job.
# Example (Windows):
#   1. Open Task Scheduler > Create Task
#   2. Set Trigger to Daily
#   3. Set Action to run: pythonw.exe path\to\fetch_and_load_contacts.py
#   4. Make sure your Python environment has requests and pyodbc installed
#
# To install dependencies:
#   pip install requests pyodbc 