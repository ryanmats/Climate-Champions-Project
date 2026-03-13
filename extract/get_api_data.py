import requests
import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

# Local CSV File paths
DEFAULT_PATH = 'data/our_data_1.csv'

# API Keys
OPENSTATES_API_KEY = 'ADD_API_KEY_HERE'
LEGISCAN_API = 'ADD_API_KEY_HERE'
BILLTRACK50_API_KEY = 'ADD_API_KEY_HERE'

# MAX LIMIT
MAX_LIMIT=400

# Google Cloud Configuration
PROJECT_ID = "climate-project-489910"
DATASET_ID = "civic_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.jurisdictions"
SESSIONS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_legiscan_sessions"
MASTERLIST_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_legiscan_masterlist_ca_2172"
BILLTRACK50_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_billtrack50_bills_ca_currentsession"
BILLTRACK50_LEGISLATORS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_billtrack50_ca_legislators"
OPENSTATES_LEGISLATORS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.src_openstates_ca_legislators"
PASSED_CLIMATE_BILLS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.stg_passed_climate_bills"
PASSED_CLIMATE_BILLS_SPONSORS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.stg_passed_climate_bills_sponsors"
PASSED_CLIMATE_BILLS_AISUMMARIES_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.stg_passed_climate_bills_aisummaries"
REPORTING_CLIMATE_CHAMPIONS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.reporting_climate_champions"
REPORTING_PASSED_CLIMATE_BILLS_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.reporting_passed_climate_bills"
GOOGLE_CLOUD_CREDENTIALS_PATH = 'service.json'

# Other Variables
CURRENT_SESSION_ID = 2172

def upload_to_bigquery(df, table_id):
    # LOAD TO BIGQUERY
    # Create credentials object from the file
    # In production, you'd likely use os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_CLOUD_CREDENTIALS_PATH)   
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE replaces the table each time (good for a list of states)
        # Use WRITE_APPEND for historical data like bill history
        write_disposition="WRITE_TRUNCATE", 
        autodetect=True,
    )

    print(f"Uploading {len(df)} rows to {table_id}...")
        
    # Start the load job
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        
    # Wait for the job to complete (Crucial for error handling)
    job.result()
    print(f"Successfully loaded data to {table_id}.")

# Create BigQuery staging table given a SQL statement and a table ID
def create_bigquery_staging_table(sql):
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_CLOUD_CREDENTIALS_PATH)   
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    query_job = client.query(sql)  # API request
    query_job.result()  # Waits for the job to complete
    print(f"BigQuery table created successfully.")

# Get the results of a BigQuery SQL query.
def get_bigquery_query_results(sql):
    credentials = service_account.Credentials.from_service_account_file(GOOGLE_CLOUD_CREDENTIALS_PATH)   
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    # Run the query
    query_job = client.query(sql) 
    
    # Wait for the job to complete and convert to a DataFrame
    # Note: Requires 'pip install pandas pyarrow db-dtypes'
    df = query_job.to_dataframe()
    
    print(f"Successfully retrieved {len(df)} rows from BigQuery.")
    return df

# Fetch Open States API Data
def get_open_states_data():

    base_url = "https://v3.openstates.org/jurisdictions"
    
    # Define headers (recommended way to pass the API key)
    headers = {
        "X-API-KEY": OPENSTATES_API_KEY
    }
    
    # Define query parameters based on the specification
    params = {
        "classification": "state",  # Filter for states only
        "include": [],  # Include session data
        "per_page": 52,  # Get all 50 states + DC + PR in one go
        "page": 1
    }

    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        # Check if the request was successful
        response.raise_for_status()
        
        data = response.json()
        
        # Print results
        for jurisdiction in data.get("results", []):
            name = jurisdiction.get("name")
            jid = jurisdiction.get("id")
            print(f"Jurisdiction: {name} ({jid})")

        # 1. Convert to DataFrame
        results = data.get("results", [])
        df = pd.DataFrame(results)
        print("Printing all results: \n")
        print(df)

        upload_to_bigquery(df, TABLE_ID)

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")


def get_legiscan_data():

    base_url = f"https://api.legiscan.com/?key={LEGISCAN_API_KEY}"
    
    # Define query parameters based on the specification
    params = {
        "op": "getSessionList",
        "state": "CA"
    }

    try:
        response = requests.get(base_url, params=params)
        
        # Check if the request was successful
        response.raise_for_status()
        
        data = response.json()
        
        # Print results
        for session in data.get("sessions", []):
            session_id = session.get("session_id")
            session_title = session.get("session_title")
            print(f"Session ID: {session_id}")
            print(f"Session Title: {session_title}")
            print(session)
            print("\n")
        
        # 1. Convert to DataFrame
        results = data.get("sessions", [])
        df = pd.DataFrame(results)
        upload_to_bigquery(df, SESSIONS_TABLE_ID)

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_legiscan_masterlist_data():

    base_url = f"https://api.legiscan.com/?key={LEGISCAN_API_KEY}"
    
    # Define query parameters based on the specification
    params = {
        "op": "getMasterList",
        "id": 2172
    }

    try:
        response = requests.get(base_url, params=params)
        
        # Check if the request was successful
        response.raise_for_status()
        
        data = response.json()

        # 1. Convert to DataFrame
        results = data.get("masterlist", [])
        # Extract the nested 'masterlist' and convert
        df = pd.DataFrame.from_dict(data['masterlist'], orient='index')
        # Reset the index if you don't want the "0", "1" strings as your index
        df.reset_index(drop=True, inplace=True)
        # Drop rows where 'bill_id' is null
        df = df.dropna(subset=['bill_id'])
        # List of metadata columns to remove
        metadata_cols = [
            'session_id', 'state_id', 'year_start', 'year_end', 
            'prefile', 'sine_die', 'prior', 'special', 
            'session_tag', 'session_title', 'session_name'
        ]

        # Drop the columns
        df = df.drop(columns=metadata_cols)
        print(df)
        
        upload_to_bigquery(df, MASTERLIST_TABLE_ID)

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_billtrack50_data():

    base_url = f"https://www.billtrack50.com/bt50api/2.1/json/bills"

    # Define headers (recommended way to pass the API key)
    headers = {
        "Authorization": f"apikey {BILLTRACK50_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Define query parameters based on the specification
    params = {
        "stateCodes": "CA",
        "searchText": "climate"
    }

    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        # Check if the request was successful
        response.raise_for_status()
        
        data = response.json()

        # 1. Convert to DataFrame
        results = data.get("bills", [])
        df = pd.DataFrame(results)
        print(df)
        
        upload_to_bigquery(df, BILLTRACK50_TABLE_ID)

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_billtrack50_california_legislators_data():

    base_url = f"https://www.billtrack50.com/bt50api/2.1/json/legislators"

    # Define headers (recommended way to pass the API key)
    headers = {
        "Authorization": f"apikey {BILLTRACK50_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Define query parameters based on the specification
    params = {
        "legislatorName": "Scott Wiener",
        "stateCodes": "CA"
    }

    try:
        response = requests.get(base_url, headers=headers, params=params)
        
        # Check if the request was successful
        response.raise_for_status()
        
        data = response.json()

        # 1. Convert to DataFrame
        results = data.get("legislators", [])
        df = pd.DataFrame(results)
        print(df)
        
        upload_to_bigquery(df, BILLTRACK50_LEGISLATORS_TABLE_ID)

    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except Exception as err:
        print(f"An error occurred: {err}")

def get_openstates_california_legislators_data():
    # This fetches a nightly-updated CSV of all current legislators for California from OpenStates.
    df_legislators = pd.read_csv("https://data.openstates.org/people/current/ca.csv")
    upload_to_bigquery(df_legislators, OPENSTATES_LEGISLATORS_TABLE_ID)

def get_passed_climate_bills_data():
    passed_climate_bills_sql = f"""
    CREATE OR REPLACE TABLE {PASSED_CLIMATE_BILLS_TABLE_ID} AS
    SELECT 'CA' as state, number as bill_number, title, status, last_action  FROM `climate-project-489910.civic_data.src_legiscan_masterlist_ca_2172` WHERE
  (REGEXP_CONTAINS(title, r'(climate|environment|emission|energy|pollution|greenhouse)') OR REGEXP_CONTAINS(description, r'(climate|environment|emission|energy|pollution|greenhouse)'))
    AND status = 4.0;
    """
    create_bigquery_staging_table(passed_climate_bills_sql)

def get_passed_climate_bills_sponsors_data():
    # 1. Get bill numbers from BigQuery
    sql = f"SELECT bill_number FROM {PASSED_CLIMATE_BILLS_TABLE_ID}"
    df = get_bigquery_query_results(sql)
    
    # Remove spaces for the v3 identifier match
    bill_list = df['bill_number'].str.replace(" ", "", regex=False).tolist()

    all_sponsors = []
    base_url = "https://v3.openstates.org/bills"
    headers = {"X-API-KEY": OPENSTATES_API_KEY}

    # 2. Break the list into chunks of 10
    chunk_size = 5 
    for i in range(0, len(bill_list), chunk_size):
        batch = bill_list[i : i + chunk_size]
        
        params = {
            "jurisdiction": "California",
            "session": "20252026", # Fixed hyphenated session
            "identifier": batch,     # Requests handles the list expansion
            "include": "sponsorships"
        }

        try:
            response = requests.get(base_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # 3. Process the results in this batch
            for bill_data in data.get("results", []):
                print(bill_data.get("sponsorships"))
                for sponsor in bill_data.get("sponsorships", []):
                    all_sponsors.append({
                        "bill_number": bill_data['identifier'],
                        "ocd_bill_id": bill_data['id'],
                        "ocd_person_id": sponsor.get("person").get("id"),
                        "sponsor_name": sponsor.get("person").get("name"),
                        "party": sponsor.get("person").get("party"),
                        "role": sponsor.get("person").get("current_role").get("title"),
                        "district": sponsor.get("person").get("current_role").get("district"),
                        "is_primary": sponsor.get("primary", False)
                    })
            
            print(f"Processed batch {i//chunk_size + 1}...")
            
            # 4. Small delay to be polite to the API
            time.sleep(1.5)

        except Exception as e:
            print(f"Error in batch starting at index {i}: {e}")

    print(all_sponsors)
    # Convert the list of dictionaries to a DataFrame
    df_sponsors = pd.DataFrame(all_sponsors)
    upload_to_bigquery(df_sponsors, PASSED_CLIMATE_BILLS_SPONSORS_TABLE_ID)


def get_billtrack50_aisummaries():

    # Get the results of a BigQuery SQL query.
    sql = f"SELECT bill_number FROM {PASSED_CLIMATE_BILLS_TABLE_ID}"
    passed_climate_bills_df = get_bigquery_query_results(sql)
    passed_climate_bills_list = passed_climate_bills_df['bill_number']

    base_url = f"https://www.billtrack50.com/bt50api/2.1/json/bills"

    # Define headers (recommended way to pass the API key)
    headers = {
        "Authorization": f"apikey {BILLTRACK50_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Your list of bills
    bill_numbers = ["SB978", "SB887", "SB949"]
    all_summaries = []

    for bill_number in passed_climate_bills_list:
        # Use 'searchText' for the specific bill number
        # StateCodes and SessionID are vital to avoid duplicates from other years/states
        params = {
            "searchText": bill_number,
            "stateCodes": "CA"
        }
        
        response = requests.get(
            "https://www.billtrack50.com/BT50Api/2.1/json/bills",
            headers={"Authorization": f"apikey {BILLTRACK50_API_KEY}"},
            params=params
        )
        
        if response.status_code == 200:
            data = response.json().get("bills", [])
            if data:
                # The first result is usually the exact match
                bill = data[0]
                all_summaries.append({
                    "bill_number": bill_number,
                    "ai_summary": bill.get("aiSummary")
                })
                print(f"✅ Retrieved summary for {bill_number}")
            else:
                print(f"⚠️ No result found for {bill_number}")
                
        # Respect the 5 req/sec limit
        time.sleep(0.25)

    # Convert to final DataFrame
    df_summaries = pd.DataFrame(all_summaries)
    print(df_summaries)

    # Send to BigQuery Staging Table
    upload_to_bigquery(df_summaries, PASSED_CLIMATE_BILLS_AISUMMARIES_TABLE_ID)

def create_reporting_table_climate_champions():
    reporting_table_climate_champions_sql = f"""
    CREATE OR REPLACE TABLE {REPORTING_CLIMATE_CHAMPIONS_TABLE_ID} AS (
        WITH top10_climate_champions AS (
            SELECT
                sponsor_name,
                party,
                COUNT(*) AS climate_bills_passed
            FROM `{PASSED_CLIMATE_BILLS_SPONSORS_TABLE_ID}`
            GROUP BY sponsor_name, party
            ORDER BY climate_bills_passed DESC
            LIMIT 10
        )
        
        SELECT
            top10.sponsor_name,
            top10.party,
            top10.climate_bills_passed,
            legislators.current_district AS district,
            legislators.current_chamber AS chamber,
            legislators.gender,
            legislators.email,
            legislators.birth_date,
            legislators.image,
            legislators.district_address,
            legislators.district_voice
        FROM top10_climate_champions AS top10
        LEFT JOIN `{OPENSTATES_LEGISLATORS_TABLE_ID}` AS legislators
        ON top10.sponsor_name = legislators.name
    );
    """
    create_bigquery_staging_table(reporting_table_climate_champions_sql)

def create_reporting_table_passed_climate_bills():
    reporting_table_passed_climate_bills_sql = f"""
    CREATE OR REPLACE TABLE {REPORTING_PASSED_CLIMATE_BILLS_TABLE_ID} AS (
        SELECT
            b.bill_number,
            b.title,
            a.ai_summary
        FROM `{PASSED_CLIMATE_BILLS_TABLE_ID}` AS b
        LEFT JOIN `{PASSED_CLIMATE_BILLS_AISUMMARIES_TABLE_ID}` AS a
        ON b.bill_number = a.bill_number
    );
    """
    create_bigquery_staging_table(reporting_table_passed_climate_bills_sql)


if __name__ == "__main__":
    # get_open_states_data()
    #get_legiscan_data()
    #get_legiscan_masterlist_data()
    #get_billtrack50_data()
    #get_billtrack50_california_legislators_data()
    #get_openstates_california_legislators_data()
    #get_passed_climate_bills_data()
    #get_passed_climate_bills_sponsors_data()
    #get_billtrack50_aisummaries()
    #create_reporting_table_climate_champions()
    create_reporting_table_passed_climate_bills()