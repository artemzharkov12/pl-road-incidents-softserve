import json
import time
import requests
import schedule

DATABRICKS_HOST = "Secret"
DATABRICKS_TOKEN = "Secret"
SQL_WAREHOUSE_ID = "Secret"
TARGET_TABLE = "Secret"

TOMTOM_API_KEY = "Secret"
TOMTOM_URL = "Secret"

# Сoordinateы for Poland
MIN_LON, MIN_LAT = 14.12, 49.00
MAX_LON, MAX_LAT = 24.15, 54.84
GRID_STEP_LON, GRID_STEP_LAT = 1.2, 0.8

def fetch_traffic_data_from_api():
    """Fetches traffic incidents from the TomTom API using a map grid."""
    print("Fetching data from TomTom API.")
    unique_records = []
    current_lon = MIN_LON

    # Loop through the map using a grid to avoid API response limits
    while current_lon < MAX_LON:
        current_lat = MIN_LAT
        
        while current_lat < MAX_LAT:
            # Calculate the boundaries for the current small map square
            next_lon = min(current_lon + GRID_STEP_LON, MAX_LON)
            next_lat = min(current_lat + GRID_STEP_LAT, MAX_LAT)
            bounding_box = f"{current_lon},{current_lat},{next_lon},{next_lat}"

            api_parameters = {
                "key": TOMTOM_API_KEY,
                "bbox": bounding_box,
                "language": "en-US", 
                "fields": "{incidents{geometry{coordinates},properties{id,iconCategory,magnitudeOfDelay,startTime,events{description}}}}",
            }

            try:
                response = requests.get(TOMTOM_URL, params=api_parameters, timeout=20)
                if response.status_code == 200:
                    incidents = response.json().get("incidents", [])
                    
                    # Filter and save only unique incident records
                    for incident in incidents:
                        incident_json_string = json.dumps(incident, ensure_ascii=False)
                        if incident_json_string not in unique_records:
                            unique_records.append(incident_json_string)
            except Exception:
                pass 

            current_lat += GRID_STEP_LAT
            time.sleep(0.2) 
        
        current_lon += GRID_STEP_LON

    print(f"Done fetching! Found {len(unique_records)} unique incidents.")
    return unique_records

def push_data_to_databricks(records):
    """Sends data directly to a Databricks table using SQL API (Zero-Bus)."""
    if not records:
        print("No new records to push. Skipping Databricks update.")
        return

    # Format records for SQL
    sql_values_list = []
    for record in records:
        safe_record = record.replace("'", "''")
        sql_values_list.append(f"('{safe_record}', current_timestamp())")
        
    sql_values_string = ", ".join(sql_values_list)

    # Idempotent inserts only 
    sql_query = f"""
    MERGE INTO {TARGET_TABLE} target
    USING (
        SELECT json_payload, ingest_timestamp 
        FROM VALUES {sql_values_string} AS data(json_payload, ingest_timestamp)
    ) source
    ON target.json_payload = source.json_payload
    WHEN NOT MATCHED THEN 
        INSERT (json_payload, ingest_timestamp) 
        VALUES (source.json_payload, source.ingest_timestamp)
    """
    
    request_headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    request_body = {
        "statement": sql_query,
        "warehouse_id": SQL_WAREHOUSE_ID
    }

    try:
        response = requests.post(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements",
            headers=request_headers,
            json=request_body
        )
        
        if response.status_code == 200:
            print(f"Pushed {len(records)} records to Databricks.")
        else:
            print(f"Error details {response.text}")
            
    except Exception as e:
        print(f"Connection error while pushing data: {e}")

def run_pipeline():
    """Main function to execute the fetch and push process."""
    print("\nStarting new pipeline run")
    traffic_data = fetch_traffic_data_from_api()
    push_data_to_databricks(traffic_data)
    print("Pipeline run finished")

schedule.every(2).minutes.do(run_pipeline)

run_pipeline() 

while True:
    schedule.run_pending()
    time.sleep(1)